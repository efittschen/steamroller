import re
from abc import ABC, abstractmethod
import typing
import os
import os.path
import hashlib
import logging
import subprocess
import shlex
from SCons.Builder import Builder
from SCons.Action import Action, CommandAction
import SCons.Subst


logger = logging.getLogger("steamroller")


def prepare_commands(target, source, env, commands):
    escape = env.get('ESCAPE', lambda x: x)
    escape_list = SCons.Subst.escape_list
    cmd_listsA = [env.subst_list(c, SCons.Subst.SUBST_CMD, target=target, source=source) for c in commands]
    cmd_listsB = [escape_list(c[0], escape) for c in cmd_listsA]
    return [' '.join(c) for c in cmd_listsB]

def create_name(target, source, env):
    m = hashlib.md5()
    m.update(bytes(" ".join([x.get_abspath() for x in target] + [x.get_abspath() for x in source]), "utf-8"))
    return "{}_{}".format(env["STEAMROLLER_NAME_PREFIX"], m.hexdigest())

# check squeue --me for job names, and check if the current job name is present
def job_exists(job_name: str) -> bool:
    pid = subprocess.Popen(["squeue", "--me", "--name", job_name, "--noheader"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pid.communicate()
    return len(stdout.strip()) > 0

def submit(
        submit_string,
        target,
        source,
        env,
        commands,
        dependencies=[],
        working_dir=None,
):
    name = create_name(target, source, env)
    log_file = "{}.log".format(target[0].abspath)
    renv = env.Override(
        {
            "STEAMROLLER_NAME" : name,
            "STEAMROLLER_LOG" : log_file,
            "STEAMROLLER_WORKING_DIRECTORY" : working_dir,
            "STEAMROLLER_DEPENDENCIES" : dependencies,
            "STEAMROLLER_SUBMIT_STRING" : submit_string
        }
    )
    submit_string = renv.subst(renv["STEAMROLLER_SUBMIT_STRING"])
    logger.debug(submit_string)
    p = subprocess.Popen(
        shlex.split(submit_string),
        stdout=subprocess.PIPE,
        stdin=subprocess.PIPE
    )
    out, err = p.communicate("\n".join([renv["STEAMROLLER_SHELL"]] + commands).encode())
    return int(out.strip())


def create_method(generator, chdir, submit_string):
    def method(target, source, env):
        commands = prepare_commands(target, source, env, generator(target, source, env, False))
        if chdir:
            nchdir = env.Dir(chdir).abspath
        else:
            nchdir = os.getcwd()
        depends_on = set(filter(lambda x : x != None, [s.GetTag("built_by_job") for s in source]))            
        job_id = submit(
            submit_string,
            target,
            source,
            env,
            commands,
            depends_on,
            working_dir=nchdir,
        )
        for t in target:
            t.Tag("built_by_job", job_id)
        logger.info("Job %d depends on %s", job_id, depends_on)
    return method


class GridEngine(ABC):
    """
    
    """

    pseudo_id = 0
    parameters = ["MEMORY", "TIME", "QUEUE", "ACCOUNT", "GPU_COUNT", "DEPENDENCIES", "LABEL_PREFIX", "WORKING_DIRECTORY", "NODELIST", "EXCLUDE"]
    
    @classmethod
    def check_for_executable(cls, name: str) -> bool:
        return any([os.path.exists(os.path.join(p, name)) for p in os.environ["PATH"].split(":")])
        
    @classmethod
    @abstractmethod
    def available(cls, *argv, **argd) -> bool:
        raise NotImplemented()
    
    def create_builder(self, env, builder, *argv, **argd):
        
        commands = builder.action.presub_lines(env)
        chdir = builder.action.chdir

        m = re.match(r"^\s*(\S*[Pp]ython3?)\s+(.*?\.py)\s+(.*)$", commands[0])
        if not m:
            raise Exception("Could not parse command: '{}'".format(commands[0]))
        
        interpreter, script, args = m.groups()
        if not os.path.exists(script):
            raise Exception("No such file: '{}'".format(script))
        
        generator = self.create_generator(commands)
        action = Action(
                create_method(generator, chdir, self.submit_string),
                self.create_command_printer(generator),
            )
        def get_contents(target, source, env):
            commands = generator(target, source, env, False)
            action = Action(commands)
            return action.get_contents(target, source, env)
        action.gc = get_contents

        return Builder(
            action=action,
            emitter=self.create_emitter(script, action.gc),
        )
    
    def create_name(self, target, source, env):
        m = hashlib.md5()
        m.update(bytes(" ".join([x.get_abspath() for x in target] + [x.get_abspath() for x in source]), "utf-8"))
        return "{}_{}".format(env["STEAMROLLER_NAME_PREFIX"], m.hexdigest())
    
    def create_generator(self, commands):
        def generator(target, source, env, for_signature):
            return commands
        return generator
    
    def create_commands(self, commands):
        pass
    
    def create_emitter(self, script, get_contents, other_deps=[]):
        def emitter(target, source, env):
            content = get_contents(target, source, env)
            content_hash = hashlib.md5()
            content_hash.update(content)
            hash_str = content_hash.hexdigest()
            hash_str = hash_str[:8]

            # rename targets to be command hash dependent
            new_targets = []
            for t in target:
                base, ext = os.path.splitext(t.get_abspath())
                new_name = "{}_{}{}".format(base, hash_str, ext)
                # if t is File object, create new File object if its Dir create new Dir object
                if isinstance(t, SCons.Node.FS.File):
                    new_t = env.File(new_name)
                elif isinstance(t, SCons.Node.FS.Dir):
                    new_t = env.Dir(new_name)
                else:
                    raise Exception("Unknown target type: {}".format(type(t)))
                new_targets.append(new_t)
            target = new_targets

            new_command = get_contents(target, source, env).decode()

            # write command to a log file
            for t in target:
                base, ext = os.path.splitext(t.get_abspath())
                log_name = "{}_{}.command".format(base, ext.lstrip("."))
                os.makedirs(os.path.dirname(log_name), exist_ok=True)
                # reformat for readability
                command = new_command.replace(" --", "\n  --")
                with open(log_name, "w") as f:
                    f.write(command)
            
            [env.Depends(t, s) for t in target for s in other_deps + [script]]
            return (target, source)
        return emitter

    def create_command_printer(self, generator):
        def command_printer(target, source, env):
            commands = prepare_commands(target, source, env, generator(target, source, env, False))
            depends_on = set(filter(lambda x : x != None, [s.GetTag("built_by_pseudo_job") for s in source]))
            job_id = self.pseudo_id
            for t in target:
                t.Tag("built_by_pseudo_job", job_id)
            self.pseudo_id += 1
            return "{0}({1})".format(                
                self.name.title(),
                ", ".join(
                    [
                        "commands={}".format(commands),
                        "name={}".format(self.create_name(target, source, env)),
                        "job_id={}".format(job_id),
                        "depends_on_jobs={}".format(depends_on),
                    ]# + [
                    #    "{}={}".format(k, v) for k, v in self.parameters.items() if v
                    #]
                )
            )
        return command_printer
