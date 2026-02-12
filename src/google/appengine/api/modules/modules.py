#!/usr/bin/env python
#
# Copyright 2007 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Exposes methods to control services (modules) and versions of an app.

Services were formerly known as modules and the API methods still
reflect that naming. For more information and code samples, see
Using the Modules guide:
https://cloud.google.com/appengine/docs/standard/python/using-the-modules-api.
"""

import logging
import os
import threading

from google.appengine.api import apiproxy_stub_map
from google.appengine.api.modules import modules_service_pb2
from google.appengine.runtime import apiproxy_errors
from googleapiclient import discovery, errors, http
from google_auth_httplib2 import AuthorizedHttp
import google.auth
import six
import httplib2


__all__ = [
    'Error',
    'InvalidModuleError',
    'InvalidVersionError',
    'InvalidInstancesError',
    'UnexpectedStateError',
    'TransientError',

    'get_current_module_name',
    'get_current_version_name',
    'get_current_instance_id',
    'get_modules',
    'get_versions',
    'get_default_version',
    'get_num_instances',
    'set_num_instances',
    'set_num_instances_async',
    'start_version',
    'start_version_async',
    'stop_version',
    'stop_version_async',
    'get_hostname'
]

class Error(Exception):
  """Base-class for errors in this module."""


class InvalidModuleError(Error):
  """The given module is not known to the system."""


class InvalidVersionError(Error):
  """The given module version is not known to the system."""


class InvalidInstancesError(Error):
  """The given instances value is not valid."""


class UnexpectedStateError(Error):
  """An unexpected current state was found when starting/stopping a module."""


class TransientError(Error):
  """A transient error was encountered, retry the operation."""

def _has_opted_in():
  return (os.environ.get('MODULES_USE_ADMIN_API', 'false').lower() == 'true')

def _raise_error(e):
  # Translate HTTP errors to the exceptions expected by the API
  if e.resp.status == 400:
      raise InvalidInstancesError(e) from e
  elif e.resp.status == 404:
      raise InvalidVersionError(e) from e
  elif e.resp.status >= 500:
      raise TransientError(e) from e
  else:
      raise Error(e) from e

def _get_project_id():
  project_id = os.environ.get('GAE_PROJECT') or os.environ.get(
      'GOOGLE_CLOUD_PROJECT'
  )
  if project_id is None:
    app_id = os.environ.get('GAE_APPLICATION')
    project_id = app_id.split('~', 1)[1]
  return project_id

def get_current_module_name():
  """Returns the module name of the current instance.

  If this is version "v1" of module "module5" for app "my-app", this function
  will return "module5".
  """
  return os.environ.get('GAE_SERVICE') or os.environ.get('CURRENT_MODULE_ID')


def get_current_version_name():
  """Returns the version of the current instance.

  If this is version "v1" of module "module5" for app "my-app", this function
  will return "v1".
  """
  result = os.environ.get('GAE_VERSION')
  if result:
    return result

  result = os.environ['CURRENT_VERSION_ID'].split('.')[0]
  return None if result == 'None' else result


def get_current_instance_id():
  """Returns the ID of the current instance.

  If this is instance 2 of version "v1" of module "module5" for app "my-app",
  this function will return "2".

  This is only valid for automatically-scaled modules; otherwise, `None` is
  returned.


  Returns:
    String containing the ID of the instance, or `None` if this is not an
    automatically-scaled module.
  """
  return os.environ.get('GAE_INSTANCE') or os.environ.get('INSTANCE_ID', None)


class _ThreadedRpc:
  """A class to emulate the UserRPC object for threaded operations."""

  def __init__(self, target):
    self.thread = threading.Thread(target=self._run_target, args=(target,))
    self.exception = None
    self.done = threading.Event()
    self.thread.start()

  def _run_target(self, target):
    try:
        target()
    except Exception as e:
        self.exception = e
    finally:
        self.done.set()

  def wait(self):
    self.done.wait()

  def check_success(self):
    if self.exception:
      raise self.exception

  def get_result(self):
    self.wait()
    self.check_success()
    return None


def _GetRpc():
  return apiproxy_stub_map.UserRPC('modules')

def _MakeAsyncCall(method, request, response, get_result_hook):
  rpc = _GetRpc()
  rpc.make_call(method, request, response, get_result_hook)
  return rpc

_MODULE_SERVICE_ERROR_MAP = {
    modules_service_pb2.ModulesServiceError.INVALID_INSTANCES:
        InvalidInstancesError,
    modules_service_pb2.ModulesServiceError.INVALID_MODULE:
        InvalidModuleError,
    modules_service_pb2.ModulesServiceError.INVALID_VERSION:
        InvalidVersionError,
    modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR:
        TransientError,
    modules_service_pb2.ModulesServiceError.UNEXPECTED_STATE:
        UnexpectedStateError
}

def _CheckAsyncResult(rpc, expected_application_errors,
                      ignored_application_errors):
  try:
    rpc.check_success()
  except apiproxy_errors.ApplicationError as e:
    if e.application_error in ignored_application_errors:
      logging.info(ignored_application_errors.get(e.application_error))
      return
    if e.application_error in expected_application_errors:
      mapped_error = _MODULE_SERVICE_ERROR_MAP.get(e.application_error)
      if mapped_error:
        raise mapped_error()
    raise Error(e)

def _get_admin_api_client_with_useragent(methodName):
  userAgent = 'appengine-modules-api-python-client/' + methodName
  http_client = httplib2.Http(timeout=60)
  http_client = http.set_user_agent(http_client, userAgent)
  credentials,_ = google.auth.default()
  authorized_http = AuthorizedHttp(credentials, http=http_client)
  client = discovery.build('appengine', 'v1', http=authorized_http)
  return client

def get_modules():
  """Returns a list of all modules for the application.

  Returns:
    List of strings containing the names of modules associated with this
      application.  The 'default' module will be included if it exists, as will
      the name of the module that is associated with the instance that calls
      this function.

  Raises:
    Error: If the configured project ID is invalid.
    TransientError: If there is an issue fetching the information.
  """
  if not _has_opted_in():
    return get_modules_legacy()
  
  project_id = _get_project_id()
  client = _get_admin_api_client_with_useragent('get_modules')
  request = client.apps().services().list(appsId=project_id)

  try:
    response = request.execute()
  except errors.HttpError as e:
    if e.resp.status == 404:
      raise Error(f"Project '{project_id}' not found.") from e
    _raise_error(e)

  return [service['id'] for service in response.get('services', [])]

#Legacy get_modules implementation
def get_modules_legacy():
  def _ResultHook(rpc):
    _CheckAsyncResult(rpc, [], {})


    return rpc.response.module

  request = modules_service_pb2.GetModulesRequest()
  response = modules_service_pb2.GetModulesResponse()
  return _MakeAsyncCall('GetModules', request, response,
                        _ResultHook).get_result()



def get_versions(module=None):
  """Returns a list of versions for a given module.

  Args:
    module: Module to retrieve version for, if `None` then the current module
      will be used.

  Returns:
    List of strings containing the names of versions associated with the module.
    The current version will also be included in this list.

  Raises:
    `InvalidModuleError` if the given module isn't valid, `TransientError` if
    there is an issue fetching the information.
  """
  if not _has_opted_in():
    return get_versions_legacy(module=module)

  if not module:
    module = os.environ.get('GAE_SERVICE', 'default')
  
  project_id = _get_project_id()
  client = _get_admin_api_client_with_useragent('get_versions')
  request = client.apps().services().versions().list(
      appsId=project_id, servicesId=module, view='FULL'
  )
  try:
    response = request.execute()
  except errors.HttpError as e:
    if e.resp.status == 404:
      raise InvalidModuleError(f"") from e
    _raise_error(e)

  return [version['id'] for version in response.get('versions', [])]

def get_versions_legacy(module=None):
  def _ResultHook(rpc):
    mapped_errors = [
        modules_service_pb2.ModulesServiceError.INVALID_MODULE,
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR
    ]
    _CheckAsyncResult(rpc, mapped_errors, {})


    return rpc.response.version

  request = modules_service_pb2.GetVersionsRequest()
  if module:
    request.module = module
  response = modules_service_pb2.GetVersionsResponse()
  return _MakeAsyncCall('GetVersions', request, response,
                        _ResultHook).get_result()  



def get_default_version(module=None):
  """Returns the name of the default version for the module.

  Args:
    module: Module to retrieve the default version for, if `None` then the
      current module will be used.

  Returns:
    String containing the name of the default version of the module.

  Raises:
    `InvalidModuleError` if the given module is not valid, `InvalidVersionError`
    if no default version could be found.
  """

  if not _has_opted_in():
    return get_default_version_legacy(module=module)

  if not module:
    module = os.environ.get('GAE_SERVICE', 'default')
  project = _get_project_id()
  client = _get_admin_api_client_with_useragent('get_default_version')
  request = client.apps().services().get(
    appsId=project, servicesId=module)

  try:
    response = request.execute()
  except errors.HttpError as e:
    if e.resp.status == 404:
        raise InvalidModuleError(f"") from e
    _raise_error(e)

  allocations = response.get('split', {}).get('allocations')
  maxAlloc = -1
  retVersion = None

  if allocations:
    for version, allocation in allocations.items():
      if allocation == 1.0:
        retVersion = version
        break

      if allocation > maxAlloc:
        retVersion = version
        maxAlloc = allocation
      elif allocation == maxAlloc:
        if version < retVersion:
          retVersion = version

  if retVersion is None:
    raise InvalidVersionError(f"Could not determine default version for module '{module}'.")

  return retVersion

def get_default_version_legacy(module):
  def _ResultHook(rpc):
    mapped_errors = [
        modules_service_pb2.ModulesServiceError.INVALID_MODULE,
        modules_service_pb2.ModulesServiceError.INVALID_VERSION
    ]
    _CheckAsyncResult(rpc, mapped_errors, {})
    return rpc.response.version

  request = modules_service_pb2.GetDefaultVersionRequest()
  if module:
    request.module = module
  response = modules_service_pb2.GetDefaultVersionResponse()
  return _MakeAsyncCall('GetDefaultVersion', request, response,
                        _ResultHook).get_result()


def get_num_instances(
    module=None,
    version=None):
  """Return the number of instances that are set for the given module version.

  This is only valid for fixed modules, an error will be raised for
  automatically-scaled modules.  Support for automatically-scaled modules may be
  supported in the future.

  Args:
    module: String containing the name of the module to fetch this info for, if
      `None` the module of the current instance will be used.
    version: String containing the name of the version to fetch this info for,
      if `None` the version of the current instance will be used.  If that
      version does not exist in the other module, then an `InvalidVersionError`
      is raised.

  Returns:
    The number of instances that are set for the given module version.

  Raises:
    `InvalidVersionError` on invalid input.
  """
  if not _has_opted_in():
    return get_num_instances_legacy(module=module, version=version)

  if module is None:
    module = get_current_module_name()

  if version is None:
    version = get_current_version_name()

  project_id = _get_project_id()
  client = _get_admin_api_client_with_useragent('get_num_instances')
  request = client.apps().services().versions().get(
        appsId=project_id, servicesId=module, versionsId=version)

  try:
    response = request.execute()
  except errors.HttpError as e:
    if e.resp.status == 404:
        raise InvalidModuleError(f"") from e
    _raise_error(e)

  if 'manualScaling' not in response:
      raise InvalidVersionError(f"")
  
  return response['manualScaling'].get('instances')

  
def get_num_instances_legacy(module, version):
  def _ResultHook(rpc):
    mapped_errors = [modules_service_pb2.ModulesServiceError.INVALID_VERSION]
    _CheckAsyncResult(rpc, mapped_errors, {})
    return rpc.response.instances

  request = modules_service_pb2.GetNumInstancesRequest()
  if module:
    request.module = module
  if version:
    request.version = version
  response = modules_service_pb2.GetNumInstancesResponse()
  return _MakeAsyncCall('GetNumInstances', request, response,
                        _ResultHook).get_result()


def _admin_api_version_patch(project_id, module, version, body, update_mask):
  methodName = ''
  if 'manualScaling' in body:
    methodName = 'set_num_instances'
  elif 'servingStatus' in body and body['servingStatus'] == 'SERVING':
    methodName = 'start_version'
  elif 'servingStatus' in body and body['servingStatus'] == 'STOPPED':
    methodName = 'stop_version'

  client = _get_admin_api_client_with_useragent(methodName)
  client.apps().services().versions().patch(
    appsId=project_id,
    servicesId=module,
    versionsId=version,
    updateMask=update_mask,
    body=body).execute()

def set_num_instances(
    instances,
    module=None,
    version=None):
  """Sets the number of instances on the module and version.

  Args:
    instances: The number of instances to set.
    module: The module to set the number of instances for, if `None` the current
      module will be used.
    version: The version set the number of instances for, if `None` the current
      version will be used.

  Raises:
    `InvalidVersionError` if the given module version isn't valid,
    `TransientError` if there is an issue persisting the change.
    `TypeError` if the given instances type is invalid.
  """
  rpc = set_num_instances_async(instances, module, version)
  rpc.get_result()


def set_num_instances_async(
    instances,
    module=None,
    version=None):
  """Returns a `UserRPC` to set the number of instances on the module version.

  Args:
    instances: The number of instances to set.
    module: The module to set the number of instances for, if `None` the current
      module will be used.
    version: The version set the number of instances for, if `None` the current
      version will be used.

  Returns:
    A `UserRPC` to set the number of instances on the module version.
  """

  if not _has_opted_in():
    return set_num_instances_async_legacy(instances=instances, module=module, version=version)

  if not isinstance(instances, six.integer_types):
    raise TypeError("'instances' arg must be of type long or int.")

  project_id = _get_project_id()
  if module is None:
    module = get_current_module_name()
  if version is None:
    version = get_current_version_name()

  def run_request():
    """This function will be executed in a separate thread."""
    try:
      body = {
        'manualScaling': {
          'instances': instances
          }
        }
      _admin_api_version_patch(project_id, module, version, body, 'manualScaling.instances')
    except errors.HttpError as e:
      _raise_error(e)

  return _ThreadedRpc(target=run_request)

def set_num_instances_async_legacy(instances, module, version):
  def _ResultHook(rpc):
    mapped_errors = [
        modules_service_pb2.ModulesServiceError.INVALID_VERSION,
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR
    ]
    _CheckAsyncResult(rpc, mapped_errors, {})

  if not isinstance(instances, six.integer_types):
    raise TypeError("'instances' arg must be of type long or int.")
  request = modules_service_pb2.SetNumInstancesRequest()
  request.instances = instances
  if module:
    request.module = module
  if version:
    request.version = version
  response = modules_service_pb2.SetNumInstancesResponse()
  return _MakeAsyncCall('SetNumInstances', request, response, _ResultHook)


def start_version(module, version):
  """Start all instances for the given version of the module.

  Args:
    module: String containing the name of the module to affect.
    version: String containing the name of the version of the module to start.

  Raises:
    `InvalidVersionError` if the given module version is invalid.
    `TransientError` if there is a problem persisting the change.
  """
  rpc = start_version_async(module, version)
  rpc.get_result()


def start_version_async(
    module,
    version):
  """Returns a `UserRPC` to start all instances for the given module version.

  Args:
    module: String containing the name of the module to affect.
    version: String containing the name of the version of the module to start.

  Returns:
    A `UserRPC` to start all instances for the given module version.
  """
  if not _has_opted_in():
    return start_version_async_legacy(module=module, version=version)

  if module is None:
    module = get_current_module_name()

  if version is None:
    version = get_current_version_name()
  project_id = _get_project_id()
  def run_request():
    """This function will be executed in a separate thread."""
    try:
      body = {
        'servingStatus': 'SERVING'
        }
      _admin_api_version_patch(project_id, module, version, body, 'servingStatus')
    except errors.HttpError as e:
      _raise_error(e)

  return _ThreadedRpc(target=run_request)

def start_version_async_legacy(module, version):
  def _ResultHook(rpc):
    mapped_errors = [
        modules_service_pb2.ModulesServiceError.INVALID_VERSION,
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR
    ]
    expected_errors = {
        modules_service_pb2.ModulesServiceError.UNEXPECTED_STATE:
            'The specified module: %s, version: %s is already started.' %
            (module, version)
    }
    _CheckAsyncResult(rpc, mapped_errors, expected_errors)

  request = modules_service_pb2.StartModuleRequest()
  request.module = module
  request.version = version
  response = modules_service_pb2.StartModuleResponse()
  return _MakeAsyncCall('StartModule', request, response, _ResultHook)

def stop_version(
    module=None,
    version=None):
  """Stops all instances for the given version of the module.

  Args:
    module: The module to affect, if `None` the current module is used.
    version: The version of the given module to affect, if `None` the current
      version is used.

  Raises:
    `InvalidVersionError` if the given module version is invalid.
    `TransientError` if there is a problem persisting the change.
  """
  rpc = stop_version_async(module, version)
  rpc.get_result()


def stop_version_async(
    module=None,
    version=None):
  """Returns a `UserRPC` to stop all instances for the given module version.

  Args:
    module: The module to affect, if `None` the current module is used.
    version: The version of the given module to affect, if `None` the current
      version is used.

  Returns:
    A `UserRPC` to stop all instances for the given module version.
  """

  if not _has_opted_in():
    return stop_version_async_legacy(module=module, version=version)

  if module is None:
    module = get_current_module_name()

  if version is None:
    version = get_current_version_name()
  project_id = _get_project_id()
  def run_request():
    """This function will be executed in a separate thread."""
    try:
      body = {
        'servingStatus': 'STOPPED'
        }
      _admin_api_version_patch(project_id, module, version, body, 'servingStatus')
    except errors.HttpError as e:
      _raise_error(e)

  return _ThreadedRpc(target=run_request)

def stop_version_async_legacy(module, version):
  def _ResultHook(rpc):
    mapped_errors = [
        modules_service_pb2.ModulesServiceError.INVALID_VERSION,
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR
    ]
    expected_errors = {
        modules_service_pb2.ModulesServiceError.UNEXPECTED_STATE:
            'The specified module: %s, version: %s is already stopped.' %
            (module, version)
    }
    _CheckAsyncResult(rpc, mapped_errors, expected_errors)
    
  request = modules_service_pb2.StopModuleRequest()
  if module:
    request.module = module
  if version:
    request.version = version
  response = modules_service_pb2.StopModuleResponse()
  return _MakeAsyncCall('StopModule', request, response, _ResultHook)


def _construct_hostname(*hostname_parts):
  """Constructs a hostname for the given module, version, and instance."""
  return ".".join(hostname_parts)

def get_hostname(
    module=None,
    version=None,
    instance=None):
  """Returns a hostname to use to contact the module.

  Args:
    module: Name of module, if None, take module of the current instance.
    version: Name of version, if version is `None` then either use the version
      of the current instance if that version exists for the target module,
      otherwise use the default version of the target module.
    instance: Instance to construct a hostname for, if instance is None, a
      loadbalanced hostname for the module will be returned.  If the target
      module is not a fixed module, then instance is not considered valid.

  Returns:
    A valid canonical hostname that can be used to communicate with the given
    module/version/instance.  For example: `0.v1.module5.myapp.appspot.com`

  Raises:
    InvalidModuleError: if the given module version is invalid.
    InvalidInstancesError: if the given instance value is invalid.
    TypeError: if the given instance type is invalid.
  """
  if not _has_opted_in():
    return get_hostname_legacy(module=module, version=version, instance=instance)

  if instance is not None:
    try:
      instance_id = int(instance)
      if instance_id < 0:
        raise ValueError
    except (ValueError, TypeError) as e:
      raise InvalidInstancesError("Instance must be a non-negative integer.") from e

  project_id = _get_project_id()
  

  req_module = module or get_current_module_name()
  req_version = version or get_current_version_name()

  try:
    services = get_modules()
    client = _get_admin_api_client_with_useragent('get_hostname')
    request = client.apps().get(appsId=project_id)
    
    response = request.execute()
    default_hostname = response.get('defaultHostname')

  except errors.HttpError as e:
    _raise_error(e)

  if req_module not in services:
    raise InvalidModuleError(f"")
  # Legacy Applications (Without "Engines")
  if len(services) == 1 and services[0] == 'default':
    if req_module != 'default':
      raise InvalidModuleError(f"Module '{req_module}' not found.")
    hostname_parts = [req_version, default_hostname]
    if instance:
      return _construct_hostname(instance, req_version, default_hostname)
    return _construct_hostname(req_version, default_hostname)

  if instance is not None:
    try:
      # Get version details to check scaling and instance count
      version_request = discovery.build('appengine', 'v1').apps().services().versions().get(
          appsId=project_id, servicesId=req_module, versionsId=req_version, view='FULL')
      version_details = version_request.execute()

      if 'manualScaling' not in version_details:
        raise InvalidInstancesError(
            "Instance-specific hostnames are only available for manually scaled services.")

      num_instances = version_details['manualScaling'].get('instances', 0)
      if int(instance) >= num_instances:
        raise InvalidInstancesError(
            "The specified instance does not exist for this module/version.")

      return _construct_hostname(instance, req_version, req_module, default_hostname)

    except errors.HttpError as e:
      if e.resp.status == 404:
        raise InvalidModuleError(
            f"Module '{req_module}' or version '{req_version}' not found.") from e
      _raise_error(e)

  # Request with no explicit version and no instance.
  if version is None:
    try:
        # Get all versions for the target module.
        versions_list = get_versions(module=req_module)

        # Create a set of version IDs for efficient lookup.
        existing_version_ids = set(versions_list)

        # Check if the version from the current context exists in the target module.
        if req_version in existing_version_ids:
            return _construct_hostname(req_version, req_module, default_hostname)
        else:
            # If the current version does not exist on the target module,
            # return a hostname without a version.
            return _construct_hostname(req_module, default_hostname)

    except errors.HttpError as e:
        if e.resp.status == 404:
            raise InvalidModuleError(f"Module '{req_module}' not found.") from e
        _raise_error(e)

  # Request with a version but no instance
  return _construct_hostname(version, req_module, default_hostname)

def get_hostname_legacy(module, version, instance):
  def _ResultHook(rpc):
    mapped_errors = [
        modules_service_pb2.ModulesServiceError.INVALID_MODULE,
        modules_service_pb2.ModulesServiceError.INVALID_INSTANCES
    ]
    _CheckAsyncResult(rpc, mapped_errors, [])
    return rpc.response.hostname

  request = modules_service_pb2.GetHostnameRequest()
  if module:
    request.module = module
  if version:
    request.version = version
  if instance or instance == 0:
    if not isinstance(instance, (six.string_types, six.integer_types)):
      raise TypeError("'instance' arg must be of type basestring, long or int.")
    request.instance = str(instance)
  response = modules_service_pb2.GetHostnameResponse()
  return _MakeAsyncCall('GetHostname', request, response,
                        _ResultHook).get_result()
