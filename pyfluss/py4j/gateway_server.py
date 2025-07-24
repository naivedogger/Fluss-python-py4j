################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import importlib.resources
import os
import platform
import signal
import subprocess
from subprocess import Popen, PIPE
from pyfluss.py4j.util import constants


def on_windows():
    return platform.system() == "Windows"


def find_java_executable():
    """Find Java executable in JAVA_HOME or PATH."""
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        java_executable = os.path.join(java_home, 'bin', 'java')
        if on_windows():
            java_executable += '.exe'
        if os.path.isfile(java_executable):
            return java_executable
    
    # Try to find java in PATH
    try:
        result = subprocess.run(['which', 'java'], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    
    raise EnvironmentError("Java executable not found. Please set JAVA_HOME or add java to PATH.")


def launch_gateway_server_process(env):
    """Launch the Java gateway server process."""
    java_executable = find_java_executable()
    
    # Build classpath
    classpath = _get_classpath(env)
    
    # Build command
    command = [
        java_executable,
        '-cp', classpath,
        constants.PYFLUSS_MAIN_CLASS
    ]
    
    # Add main args if specified
    if constants.PYFLUSS_MAIN_ARGS in env:
        command.extend(env[constants.PYFLUSS_MAIN_ARGS].split())
    
    # Start the process
    return Popen(command, stdout=PIPE, stderr=PIPE, env=env)


_JAVA_DEPS_PACKAGE = 'pyfluss.jars'


def _get_classpath(env):
    """Build Java classpath for the gateway server."""
    classpath = []

    # note that jars are not packaged in test
    test_mode = os.environ.get(constants.PYFLUSS4J_TEST_MODE)
    if not test_mode or test_mode.lower() != "true":
        try:
            jars = importlib.resources.files(_JAVA_DEPS_PACKAGE)
            one_jar = next(iter(jars.iterdir()), None)
            if not one_jar:
                raise ValueError("Haven't found necessary python-java-bridge jar, this is unexpected.")
            builtin_java_classpath = os.path.join(os.path.dirname(str(one_jar)), '*')
            classpath.append(builtin_java_classpath)
        except Exception:
            # In development mode, jars might not be packaged
            pass

    # user defined
    if constants.PYFLUSS_JAVA_CLASSPATH in env:
        classpath.append(env[constants.PYFLUSS_JAVA_CLASSPATH])

    # hadoop
    hadoop_classpath = _get_hadoop_classpath(env)
    if hadoop_classpath is not None:
        classpath.append(hadoop_classpath)

    return os.pathsep.join(classpath)


_HADOOP_DEPS_PACKAGE = 'pyfluss.hadoop-deps'


def _get_hadoop_classpath(env):
    """Get Hadoop classpath."""
    if constants.PYFLUSS_HADOOP_CLASSPATH in env:
        return env[constants.PYFLUSS_HADOOP_CLASSPATH]
    elif 'HADOOP_CLASSPATH' in env:
        return env['HADOOP_CLASSPATH']
    else:
        # use built-in hadoop
        try:
            jars = importlib.resources.files(_HADOOP_DEPS_PACKAGE)
            one_jar = next(iter(jars.iterdir()), None)
            if not one_jar:
                raise EnvironmentError(f"The built-in Hadoop environment has been broken, this "
                                     f"is unexpected. You can set one of '{constants.PYFLUSS_HADOOP_CLASSPATH}' or "
                                     f"'HADOOP_CLASSPATH' to continue.")
            return os.path.join(os.path.dirname(str(one_jar)), '*')
        except Exception:
            # In development mode, hadoop deps might not be packaged
            return None
