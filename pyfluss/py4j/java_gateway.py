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

import atexit
import os
import signal
import time
from py4j.java_gateway import JavaGateway, GatewayParameters
from .gateway_server import launch_gateway_server_process
from .util import constants

_gateway = None
_gateway_proc = None


def get_gateway():
    """Get or create the py4j gateway to Java."""
    global _gateway, _gateway_proc
    
    if _gateway is not None:
        return _gateway
    
    # Check if we're in test mode
    test_mode = os.environ.get(constants.PYFLUSS4J_TEST_MODE)
    if test_mode and test_mode.lower() == "true":
        # In test mode, assume gateway is already running
        _gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_convert=True))
        return _gateway
    
    # Launch gateway server process
    env = dict(os.environ)
    _gateway_proc = launch_gateway_server_process(env)
    
    # Wait a bit for the server to start
    time.sleep(2)
    
    # Connect to the gateway
    _gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_convert=True))
    
    # Register cleanup function
    atexit.register(_cleanup_gateway)
    
    # Handle signals for cleanup
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, _signal_handler)
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, _signal_handler)
    
    return _gateway


def _cleanup_gateway():
    """Clean up the gateway and its process."""
    global _gateway, _gateway_proc
    
    if _gateway is not None:
        try:
            _gateway.shutdown()
        except Exception:
            pass
        _gateway = None
    
    if _gateway_proc is not None:
        try:
            _gateway_proc.terminate()
            _gateway_proc.wait(timeout=5)
        except Exception:
            try:
                _gateway_proc.kill()
            except Exception:
                pass
        _gateway_proc = None


def _signal_handler(signum, frame):
    """Handle signals for graceful shutdown."""
    _cleanup_gateway()
    exit(0)
