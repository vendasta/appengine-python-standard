#!/usr/bin/env python
#
# Copyright 2007 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for google.appengine.api.modules."""

import logging
import os

import google

from google.appengine.api.modules import modules
from google.appengine.api.modules import modules_service_pb2
from google.appengine.runtime import apiproxy_errors
from google.appengine.runtime.context import ctx_test_util
import google.auth
import google_auth_httplib2
from googleapiclient import discovery
import mox

from absl.testing import absltest
from googleapiclient import errors


@ctx_test_util.isolated_context()
class ModulesTest(absltest.TestCase):

  def setUp(self):
    """Setup testing environment."""
    self.mox = mox.Mox()
    self.mock_admin_api_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, 'discovery')

    # Environment variables are cleared in tearDown
    os.environ['GAE_APPLICATION'] = 's~project'
    os.environ['GOOGLE_CLOUD_PROJECT'] = 'project'
    os.environ['GAE_SERVICE'] = 'default'
    os.environ['GAE_VERSION'] = 'v1'
    os.environ['CURRENT_MODULE_ID'] = 'default'
    os.environ['CURRENT_VERSION_ID'] = 'v1.123'

  def tearDown(self):
    """Tear down testing environment."""
    self.mox.UnsetStubs()
    self.mox.VerifyAll()

    # Clear environment variables that were set in tests
    for var in [
        'GAE_SERVICE', 'GAE_VERSION', 'CURRENT_MODULE_ID', 'CURRENT_VERSION_ID',
        'INSTANCE_ID', 'GAE_INSTANCE', 'GOOGLE_CLOUD_PROJECT', 'GAE_APPLICATION'
    ]:
      if var in os.environ:
        del os.environ[var]

  def _SetupAdminApiMocks(self, project='project'):
    modules.discovery.build('appengine',
                            'v1').AndReturn(self.mock_admin_api_client)

  def _CreateHttpError(self, status, reason='Error'):
    resp = self.mox.CreateMockAnything()
    resp.status = status
    resp.reason = reason
    return errors.HttpError(resp, b'')

  # --- Tests for Get/Set Current Module, Version, Instance ---

  def testGetCurrentModuleName(self):
    os.environ['GAE_SERVICE'] = 'module1'
    self.assertEqual('module1', modules.get_current_module_name())

  def testGetCurrentModuleName_Fallback(self):
    if 'GAE_SERVICE' in os.environ:
      del os.environ['GAE_SERVICE']
    os.environ['CURRENT_MODULE_ID'] = 'module2'
    self.assertEqual('module2', modules.get_current_module_name())

  def testGetCurrentVersionName(self):
    os.environ['GAE_VERSION'] = 'v2'
    self.assertEqual('v2', modules.get_current_version_name())

  def testGetCurrentVersionName_Fallback(self):
    if 'GAE_VERSION' in os.environ:
      del os.environ['GAE_VERSION']
    os.environ['CURRENT_VERSION_ID'] = 'v3.456'
    self.assertEqual('v3', modules.get_current_version_name())

  def testGetCurrentVersionName_None(self):
    if 'GAE_VERSION' in os.environ:
      del os.environ['GAE_VERSION']
    os.environ['CURRENT_VERSION_ID'] = 'None.456'
    self.assertIsNone(modules.get_current_version_name())

  def testGetCurrentInstanceId(self):
    os.environ['GAE_INSTANCE'] = 'instance1'
    self.assertEqual('instance1', modules.get_current_instance_id())

  def testGetCurrentInstanceId_Fallback(self):
    if 'GAE_INSTANCE' in os.environ:
        del os.environ['GAE_INSTANCE']
    os.environ['INSTANCE_ID'] = 'instance2'
    self.assertEqual('instance2', modules.get_current_instance_id())

  def testGetCurrentInstanceId_None(self):
    if 'GAE_INSTANCE' in os.environ:
      del os.environ['GAE_INSTANCE']
    if 'INSTANCE_ID' in os.environ:
      del os.environ['INSTANCE_ID']
    self.assertIsNone(modules.get_current_instance_id())

  def SetSuccessExpectations(self, method, expected_request, service_response):
    rpc = MockRpc(method, expected_request, service_response)
    self.mox.StubOutWithMock(modules, '_GetRpc')
    modules._GetRpc().AndReturn(rpc)
    self.mox.ReplayAll()

  def SetExceptionExpectations(self, method, expected_request,
                               application_error_number):
    rpc = MockRpc(method, expected_request, None, application_error_number)
    self.mox.StubOutWithMock(modules, '_GetRpc')
    modules._GetRpc().AndReturn(rpc)
    self.mox.ReplayAll()

  # --- Tests for updated get_modules ---

  def testGetModules(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_modules').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.list(appsId='project').AndReturn(mock_request)
    mock_request.execute().AndReturn(
        {'services': [{'id': 'module1'}, {'id': 'default'}]})
    self.mox.ReplayAll()
    self.assertEqual(['module1', 'default'], modules.get_modules())

  def testGetModules_InvalidProject(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_modules').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    
    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.list(appsId='project').AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(404))
    self.mox.ReplayAll()
    with self.assertRaisesRegex(modules.Error, "Project 'project' not found."):
      modules.get_modules()

  # --- Tests for legacy get_modules ---

  def testGetModulesLegacy(self):
    """Test we return the expected results."""
    service_response = modules_service_pb2.GetModulesResponse()
    service_response.module.append('module1')
    service_response.module.append('module2')
    self.SetSuccessExpectations('GetModules',
                                modules_service_pb2.GetModulesRequest(),
                                service_response)
    self.assertEqual(['module1', 'module2'], modules.get_modules())

  # --- Tests for updated get_versions ---

  def testGetVersions(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_versions').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.list(
        appsId='project', servicesId='default', view='FULL').AndReturn(
            mock_request)
    mock_request.execute().AndReturn({'versions': [{'id': 'v1'}, {'id': 'v2'}]})
    self.mox.ReplayAll()
    self.assertEqual(['v1', 'v2'], modules.get_versions())

  def testGetVersions_InvalidModule(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_versions').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.list(
        appsId='project', servicesId='foo', view='FULL').AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(404))
    self.mox.ReplayAll()
    with self.assertRaisesRegex(modules.InvalidModuleError,
                                  ""):
      modules.get_versions(module='foo')
      
  # --- Tests for Legacy get_versions ---
  
  def testGetVersionsLegacy(self):
    """Test we return the expected results."""
    expected_request = modules_service_pb2.GetVersionsRequest()
    expected_request.module = 'module1'
    service_response = modules_service_pb2.GetVersionsResponse()
    service_response.version.append('v1')
    service_response.version.append('v2')
    self.SetSuccessExpectations('GetVersions',
                                expected_request,
                                service_response)
    self.assertEqual(['v1', 'v2'], modules.get_versions('module1'))

  def testGetVersionsLegacy_NoModule(self):
    """Test we return the expected results when no module is passed."""
    expected_request = modules_service_pb2.GetVersionsRequest()
    service_response = modules_service_pb2.GetVersionsResponse()
    service_response.version.append('v1')
    service_response.version.append('v2')
    self.SetSuccessExpectations('GetVersions',
                                expected_request,
                                service_response)
    self.assertEqual(['v1', 'v2'], modules.get_versions())

  def testGetVersionsLegacy_InvalidModuleError(self):
    """Test we raise the right error when the given module is invalid."""
    self.SetExceptionExpectations(
        'GetVersions', modules_service_pb2.GetVersionsRequest(),
        modules_service_pb2.ModulesServiceError.INVALID_MODULE)
    self.assertRaises(modules.InvalidModuleError, modules.get_versions)

  def testGetVersionsLegacy_TransientError(self):
    """Test we raise the right error when a transient error is encountered."""
    self.SetExceptionExpectations(
        'GetVersions', modules_service_pb2.GetVersionsRequest(),
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR)
    self.assertRaises(modules.TransientError, modules.get_versions)

  # --- Tests for updated get_default_version ---

  def testGetDefaultVersion(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'

    mock_admin_api_client = self.mox.CreateMockAnything()

    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent(
        'get_default_version').AndReturn(mock_admin_api_client)

    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()

    mock_admin_api_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.get(appsId='project',
                      servicesId='default').AndReturn(mock_request)
    mock_request.execute().AndReturn(
        {'split': {'allocations': {'v1': 0.5, 'v2': 0.5}}})

    self.mox.ReplayAll()

    # The assertion remains the same
    self.assertEqual('v1', modules.get_default_version())

  def testGetDefaultVersion_Lexicographical(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_admin_api_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent(
        'get_default_version').AndReturn(mock_admin_api_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_admin_api_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.get(appsId='project',
                      servicesId='default').AndReturn(mock_request)
    mock_request.execute().AndReturn(
        {'split': {'allocations': {'v2-beta': 0.5, 'v1-stable': 0.5}}})
    self.mox.ReplayAll()
    self.assertEqual('v1-stable', modules.get_default_version())


  def testGetDefaultVersion_NoDefaultVersion(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_admin_api_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent(
        'get_default_version').AndReturn(mock_admin_api_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_admin_api_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.get(appsId='project',
                      servicesId='default').AndReturn(mock_request)
    mock_request.execute().AndReturn({})
    self.mox.ReplayAll()
    with self.assertRaisesRegex(modules.InvalidVersionError,
                                  'Could not determine default version'):
      modules.get_default_version()

  def testGetDefaultVersion_InvalidModule(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'

    mock_admin_api_client = self.mox.CreateMockAnything()

    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent(
        'get_default_version').AndReturn(mock_admin_api_client)

    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()

    mock_admin_api_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.get(appsId='project',
                      servicesId='foo').AndReturn(mock_request)

    mock_request.execute().AndRaise(self._CreateHttpError(404))

    self.mox.ReplayAll()

    with self.assertRaisesRegex(modules.InvalidModuleError,
                                  ""):
      modules.get_default_version(module='foo')
      
  # --- Tests for legacy get_default_version ---
  
  def testGetDefaultVersionLegacy(self):
    """Test we return the expected results."""
    expected_request = modules_service_pb2.GetDefaultVersionRequest()
    expected_request.module = 'module1'
    service_response = modules_service_pb2.GetDefaultVersionResponse()
    service_response.version = 'v1'
    self.SetSuccessExpectations('GetDefaultVersion',
                                expected_request,
                                service_response)
    self.assertEqual('v1', modules.get_default_version('module1'))

  def testGetDefaultVersionLegacy_NoModule(self):
    """Test we return the expected results when no module is passed."""
    expected_request = modules_service_pb2.GetDefaultVersionRequest()
    service_response = modules_service_pb2.GetDefaultVersionResponse()
    service_response.version = 'v1'
    self.SetSuccessExpectations('GetDefaultVersion',
                                expected_request,
                                service_response)
    self.assertEqual('v1', modules.get_default_version())

  def testGetDefaultVersionLegacy_InvalidModuleError(self):
    """Test we raise an error when one is received from the lower API."""
    self.SetExceptionExpectations(
        'GetDefaultVersion', modules_service_pb2.GetDefaultVersionRequest(),
        modules_service_pb2.ModulesServiceError.INVALID_MODULE)
    self.assertRaises(modules.InvalidModuleError, modules.get_default_version)

  def testGetDefaultVersionLegacy_InvalidVersionError(self):
    """Test we raise an error when one is received from the lower API."""
    self.SetExceptionExpectations(
        'GetDefaultVersion', modules_service_pb2.GetDefaultVersionRequest(),
        modules_service_pb2.ModulesServiceError.INVALID_VERSION)
    self.assertRaises(modules.InvalidVersionError, modules.get_default_version)
  
  # --- Tests for updated get_num_instances ---

  def testGetNumInstances(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_num_instances').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.get(appsId='project', servicesId='default',
                      versionsId='v1').AndReturn(mock_request)
    mock_request.execute().AndReturn({'manualScaling': {'instances': 5}})
    self.mox.ReplayAll()
    self.assertEqual(5, modules.get_num_instances())

  def testGetNumInstances_NoManualScaling(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_num_instances').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.get(appsId='project', servicesId='default',
                      versionsId='v1').AndReturn(mock_request)
    mock_request.execute().AndReturn({'automaticScaling': {}})
    self.mox.ReplayAll()

    with self.assertRaises(modules.InvalidVersionError):
      modules.get_num_instances()

  def testGetNumInstances_InvalidVersion(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_num_instances').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    
    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.get(appsId='project', servicesId='default',
                      versionsId='v-bad').AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(404))
    self.mox.ReplayAll()
    with self.assertRaises(modules.InvalidModuleError):
      modules.get_num_instances(version='v-bad')

  # --- Tests for updated get_num_instances ---
  
  def testGetNumInstancesLegacy(self):
    """Test we return the expected results."""
    expected_request = modules_service_pb2.GetNumInstancesRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    service_response = modules_service_pb2.GetNumInstancesResponse()
    service_response.instances = 11
    self.SetSuccessExpectations('GetNumInstances',
                                expected_request,
                                service_response)
    self.assertEqual(11, modules.get_num_instances('module1', 'v1'))

  def testGetNumInstancesLegacy_NoVersion(self):
    """Test we return the expected results when no version is passed."""
    expected_request = modules_service_pb2.GetNumInstancesRequest()
    expected_request.module = 'module1'
    service_response = modules_service_pb2.GetNumInstancesResponse()
    service_response.instances = 11
    self.SetSuccessExpectations('GetNumInstances',
                                expected_request,
                                service_response)
    self.assertEqual(11, modules.get_num_instances('module1'))

  def testGetNumInstancesLegacy_NoModule(self):
    """Test we return the expected results when no module is passed."""
    expected_request = modules_service_pb2.GetNumInstancesRequest()
    expected_request.version = 'v1'
    service_response = modules_service_pb2.GetNumInstancesResponse()
    service_response.instances = 11
    self.SetSuccessExpectations('GetNumInstances',
                                expected_request,
                                service_response)
    self.assertEqual(11, modules.get_num_instances(version='v1'))

  def testGetNumInstancesLegacy_AllDefaults(self):
    """Test we return the expected results when no args are passed."""
    expected_request = modules_service_pb2.GetNumInstancesRequest()
    service_response = modules_service_pb2.GetNumInstancesResponse()
    service_response.instances = 11
    self.SetSuccessExpectations('GetNumInstances',
                                expected_request,
                                service_response)
    self.assertEqual(11, modules.get_num_instances())

  def testGetNumInstancesLegacy_InvalidVersionError(self):
    """Test we raise the expected error when the API call fails."""
    expected_request = modules_service_pb2.GetNumInstancesRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.SetExceptionExpectations(
        'GetNumInstances', expected_request,
        modules_service_pb2.ModulesServiceError.INVALID_VERSION)
    self.assertRaises(modules.InvalidVersionError,
                      modules.get_num_instances, 'module1', 'v1')  
  
  # --- Tests for updated set_num_instances---

  def testSetNumInstances(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('set_num_instances').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v1',
        updateMask='manualScaling.instances',
        body={'manualScaling': {'instances': 10}}).AndReturn(mock_request)
    mock_request.execute()
    self.mox.ReplayAll()
    modules.set_num_instances(10)

  def testSetNumInstances_TypeError(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    with self.assertRaises(TypeError):
      modules.set_num_instances('not-an-int')

  def testSetNumInstances_InvalidInstancesError(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('set_num_instances').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v1',
        updateMask='manualScaling.instances',
        body={'manualScaling': {'instances': -1}}).AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(400))
    self.mox.ReplayAll()
    with self.assertRaises(modules.InvalidInstancesError):
      modules.set_num_instances(-1)
      
  # --- Tests for legacy set_num_instances---

  def testSetNumInstancesLegacy(self):
    """Test we return the expected results."""
    expected_request = modules_service_pb2.SetNumInstancesRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    expected_request.instances = 12
    service_response = modules_service_pb2.SetNumInstancesResponse()
    self.SetSuccessExpectations('SetNumInstances',
                                expected_request,
                                service_response)
    modules.set_num_instances(12, 'module1', 'v1')

  def testSetNumInstancesLegacy_NoVersion(self):
    """Test we return the expected results when no version is passed."""
    expected_request = modules_service_pb2.SetNumInstancesRequest()
    expected_request.module = 'module1'
    expected_request.instances = 13
    service_response = modules_service_pb2.SetNumInstancesResponse()
    self.SetSuccessExpectations('SetNumInstances',
                                expected_request,
                                service_response)
    modules.set_num_instances(13, 'module1')

  def testSetNumInstancesLegacy_NoModule(self):
    """Test we return the expected results when no module is passed."""
    expected_request = modules_service_pb2.SetNumInstancesRequest()
    expected_request.version = 'v1'
    expected_request.instances = 14
    service_response = modules_service_pb2.SetNumInstancesResponse()
    self.SetSuccessExpectations('SetNumInstances',
                                expected_request,
                                service_response)
    modules.set_num_instances(14, version='v1')

  def testSetNumInstancesLegacy_AllDefaults(self):
    """Test we return the expected results when no args are passed."""
    expected_request = modules_service_pb2.SetNumInstancesRequest()
    expected_request.instances = 15
    service_response = modules_service_pb2.SetNumInstancesResponse()
    self.SetSuccessExpectations('SetNumInstances',
                                expected_request,
                                service_response)
    modules.set_num_instances(15)

  def testSetNumInstancesLegacy_BadInstancesType(self):
    """Test we raise an error when we receive a bad instances type."""
    self.assertRaises(TypeError, modules.set_num_instances, 'no good')

  def testSetNumInstancesLegacy_InvalidVersionError(self):
    """Test we raise an error when we receive on from the underlying API."""
    expected_request = modules_service_pb2.SetNumInstancesRequest()
    expected_request.instances = 23
    self.SetExceptionExpectations(
        'SetNumInstances', expected_request,
        modules_service_pb2.ModulesServiceError.INVALID_VERSION)
    self.assertRaises(modules.InvalidVersionError,
                      modules.set_num_instances, 23)

  def testSetNumInstancesLegacy_TransientError(self):
    """Test we raise an error when we receive on from the underlying API."""
    expected_request = modules_service_pb2.SetNumInstancesRequest()
    expected_request.instances = 23
    self.SetExceptionExpectations(
        'SetNumInstances', expected_request,
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR)
    self.assertRaises(modules.TransientError, modules.set_num_instances, 23)

  # --- Tests for updated start_version---

  def testStartVersion(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('start_version').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v1',
        updateMask='servingStatus',
        body={'servingStatus': 'SERVING'}).AndReturn(mock_request)
    mock_request.execute()
    self.mox.ReplayAll()
    modules.start_version('default', 'v1')

  def testStartVersion_InvalidVersion(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('start_version').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v-bad',
        updateMask='servingStatus',
        body={'servingStatus': 'SERVING'}).AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(404))
    self.mox.ReplayAll()
    with self.assertRaises(modules.InvalidVersionError):
      modules.start_version('default', 'v-bad')

  def testStartVersionAsync_NoneArgs(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('start_version').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v1',
        updateMask='servingStatus',
        body={'servingStatus': 'SERVING'}).AndReturn(mock_request)
    mock_request.execute()
    self.mox.ReplayAll()
    rpc = modules.start_version_async(None, None)
    rpc.get_result()

  # --- Tests for legacy start_version---
  
  def testStartVersionLegacy(self):
    """Test we pass through the expected args."""
    expected_request = modules_service_pb2.StartModuleRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    service_response = modules_service_pb2.StartModuleResponse()
    self.SetSuccessExpectations('StartModule',
                                expected_request,
                                service_response)
    modules.start_version('module1', 'v1')

  def testStartVersionLegacy_InvalidVersionError(self):
    """Test we raise an error when we receive one from the API."""
    expected_request = modules_service_pb2.StartModuleRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.SetExceptionExpectations(
        'StartModule', expected_request,
        modules_service_pb2.ModulesServiceError.INVALID_VERSION)
    self.assertRaises(modules.InvalidVersionError,
                      modules.start_version,
                      'module1',
                      'v1')

  def testStartVersionLegacy_UnexpectedStateError(self):
    """Test we don't raise an error if the version is already started."""
    expected_request = modules_service_pb2.StartModuleRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.mox.StubOutWithMock(logging, 'info')
    logging.info('The specified module: module1, version: v1 is already '
                 'started.')
    self.SetExceptionExpectations(
        'StartModule', expected_request,
        modules_service_pb2.ModulesServiceError.UNEXPECTED_STATE)
    modules.start_version('module1', 'v1')

  def testStartVersionLegacy_TransientError(self):
    """Test we raise an error when we receive one from the API."""
    expected_request = modules_service_pb2.StartModuleRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.SetExceptionExpectations(
        'StartModule', expected_request,
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR)
    self.assertRaises(modules.TransientError,
                      modules.start_version,
                      'module1',
                      'v1')
  
  # --- Tests for updated stop_version---  

  def testStopVersion(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('stop_version').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v1',
        updateMask='servingStatus',
        body={'servingStatus': 'STOPPED'}).AndReturn(mock_request)
    mock_request.execute()
    self.mox.ReplayAll()
    modules.stop_version()

  def testStopVersion_InvalidVersion(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('stop_version').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v-bad',
        updateMask='servingStatus',
        body={'servingStatus': 'STOPPED'}).AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(404))
    self.mox.ReplayAll()
    with self.assertRaises(modules.InvalidVersionError):
      modules.stop_version(version='v-bad')

  def testStopVersion_TransientError(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('stop_version').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v1',
        updateMask='servingStatus',
        body={'servingStatus': 'STOPPED'}).AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(500))
    self.mox.ReplayAll()
    with self.assertRaises(modules.TransientError):
      modules.stop_version()

  # --- Tests for legacy stop_version--
  
  def testStopVersionLegacy_NoModule(self):
    """Test we pass through the expected args."""
    expected_request = modules_service_pb2.StopModuleRequest()
    expected_request.version = 'v1'
    service_response = modules_service_pb2.StopModuleResponse()
    self.SetSuccessExpectations('StopModule',
                                expected_request,
                                service_response)
    modules.stop_version(version='v1')

  def testStopVersionLegacy_NoVersion(self):
    """Test we pass through the expected args."""
    expected_request = modules_service_pb2.StopModuleRequest()
    expected_request.module = 'module1'
    service_response = modules_service_pb2.StopModuleResponse()
    self.SetSuccessExpectations('StopModule',
                                expected_request,
                                service_response)
    modules.stop_version('module1')

  def testStopVersionLegacy_InvalidVersionError(self):
    """Test we raise an error when we receive one from the API."""
    expected_request = modules_service_pb2.StopModuleRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.SetExceptionExpectations(
        'StopModule', expected_request,
        modules_service_pb2.ModulesServiceError.INVALID_VERSION)
    self.assertRaises(modules.InvalidVersionError,
                      modules.stop_version,
                      'module1',
                      'v1')

  def testStopVersionLegacy_AlreadyStopped(self):
    """Test we don't raise an error if the version is already stopped."""
    expected_request = modules_service_pb2.StopModuleRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.mox.StubOutWithMock(logging, 'info')
    logging.info('The specified module: module1, version: v1 is already '
                 'stopped.')
    self.SetExceptionExpectations(
        'StopModule', expected_request,
        modules_service_pb2.ModulesServiceError.UNEXPECTED_STATE)
    modules.stop_version('module1', 'v1')

  def testStopVersionLegacy_TransientError(self):
    """Test we raise an error when we receive one from the API."""
    self.SetExceptionExpectations(
        'StopModule', modules_service_pb2.StopModuleRequest(),
        modules_service_pb2.ModulesServiceError.TRANSIENT_ERROR)
    self.assertRaises(modules.TransientError, modules.stop_version)

  def testRaiseError_Generic(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent(mox.IsA(str)).AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.patch(
        appsId='project',
        servicesId='default',
        versionsId='v1',
        updateMask='servingStatus',
        body={'servingStatus': 'STOPPED'}).AndReturn(mock_request)
    mock_request.execute().AndRaise(self._CreateHttpError(401)) # Unauthorized
    self.mox.ReplayAll()
    with self.assertRaises(modules.Error):
        modules.stop_version()

   # --- Tests for updated get_hostname ---

  def testGetHostname_WithVersion_NoInstance(self):
    """Tests the simple case with an explicit module and version."""
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_admin_api_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent(
        'get_hostname').AndReturn(mock_admin_api_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_modules')
    modules.get_modules().AndReturn(['default', 'other', 'foo'])
    mock_apps = self.mox.CreateMockAnything()
    mock_get_request = self.mox.CreateMockAnything()
    mock_admin_api_client.apps().AndReturn(mock_apps)
    mock_apps.get(appsId='project').AndReturn(mock_get_request)
    mock_get_request.execute().AndReturn(
        {'defaultHostname': 'project.appspot.com'})
    self.mox.ReplayAll()
    self.assertEqual('v2.foo.project.appspot.com',
                     modules.get_hostname(module='foo', version='v2'))

  def testGetHostname_Instance_Success(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client_1 = self.mox.CreateMockAnything()
    mock_client_2 = self.mox.CreateMockAnything()

    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent(
        'get_hostname').AndReturn(mock_client_1)
    self.mox.StubOutWithMock(modules.discovery, 'build')
    modules.discovery.build('appengine', 'v1').AndReturn(mock_client_2)

    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_modules')
    modules.get_modules().AndReturn(['default', 'other'])
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')

    mock_apps_1 = self.mox.CreateMockAnything()
    mock_get_request = self.mox.CreateMockAnything()
    mock_client_1.apps().AndReturn(mock_apps_1)
    mock_apps_1.get(appsId='project').AndReturn(mock_get_request)
    mock_get_request.execute().AndReturn(
        {'defaultHostname': 'project.appspot.com'})

    mock_apps_2 = self.mox.CreateMockAnything()
    mock_services_2 = self.mox.CreateMockAnything()
    mock_versions_2 = self.mox.CreateMockAnything()
    mock_version_request = self.mox.CreateMockAnything()
    mock_client_2.apps().AndReturn(mock_apps_2)
    mock_apps_2.services().AndReturn(mock_services_2)
    mock_services_2.versions().AndReturn(mock_versions_2)
    mock_versions_2.get(
        appsId='project', servicesId='default', versionsId='v1',
        view='FULL').AndReturn(mock_version_request)
    mock_version_request.execute().AndReturn(
        {'manualScaling': {'instances': 5}})

    self.mox.ReplayAll()

    self.assertEqual('2.v1.default.project.appspot.com',
                     modules.get_hostname(instance='2'))


  def testGetHostname_Instance_NoManualScaling(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client_1 = self.mox.CreateMockAnything()
    mock_client_2 = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_hostname').AndReturn(mock_client_1)
    self.mox.StubOutWithMock(modules.discovery, 'build')
    modules.discovery.build('appengine', 'v1').AndReturn(mock_client_2)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_modules')
    modules.get_modules().AndReturn(['default', 'other'])
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')
    mock_apps_1 = self.mox.CreateMockAnything()
    mock_get_request = self.mox.CreateMockAnything()
    mock_client_1.apps().AndReturn(mock_apps_1)
    mock_apps_1.get(appsId='project').AndReturn(mock_get_request)
    mock_get_request.execute().AndReturn(
        {'defaultHostname': 'project.appspot.com'})
    mock_apps_2 = self.mox.CreateMockAnything()
    mock_services_2 = self.mox.CreateMockAnything()
    mock_versions_2 = self.mox.CreateMockAnything()
    mock_version_request = self.mox.CreateMockAnything()
    mock_client_2.apps().AndReturn(mock_apps_2)
    mock_apps_2.services().AndReturn(mock_services_2)
    mock_services_2.versions().AndReturn(mock_versions_2)
    mock_versions_2.get(
        appsId='project', servicesId='default', versionsId='v1',
        view='FULL').AndReturn(mock_version_request)
    mock_version_request.execute().AndReturn({'automaticScaling': {}})
    self.mox.ReplayAll()
    with self.assertRaisesRegex(
        modules.InvalidInstancesError,
        'Instance-specific hostnames are only available for manually scaled '
        'services.'):
      modules.get_hostname(instance='1')

  def testGetHostname_Instance_OutOfBounds(self):
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_api_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(google.auth, 'default')
    google.auth.default().AndReturn((None, 'project'))
    
    self.mox.StubOutWithMock(modules.discovery, 'build')
    modules.discovery.build('appengine', 'v1', http=mox.IsA(object)).AndReturn(mock_api_client)
    modules.discovery.build('appengine', 'v1').AndReturn(mock_api_client)

    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_modules')
    modules.get_modules().AndReturn(['default', 'other'])
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')
    mock_apps = self.mox.CreateMockAnything()
    mock_get_request = self.mox.CreateMockAnything()
    mock_services = self.mox.CreateMockAnything()
    mock_versions = self.mox.CreateMockAnything()
    mock_version_request = self.mox.CreateMockAnything()
    mock_api_client.apps().AndReturn(mock_apps)
    mock_apps.get(appsId='project').AndReturn(mock_get_request)
    mock_get_request.execute().AndReturn(
        {'defaultHostname': 'project.appspot.com'})
    mock_api_client.apps().AndReturn(mock_apps)
    mock_apps.services().AndReturn(mock_services)
    mock_services.versions().AndReturn(mock_versions)
    mock_versions.get(
        appsId='project', servicesId='default', versionsId='v1',
        view='FULL').AndReturn(mock_version_request)
    mock_version_request.execute().AndReturn(
        {'manualScaling': {'instances': 5}})
    self.mox.ReplayAll()
    with self.assertRaisesRegex(
        modules.InvalidInstancesError,
        'The specified instance does not exist for this module/version.'):
      modules.get_hostname(instance='5')

  def testGetHostname_Instance_InvalidValue(self):
    """Tests instance request with an invalid non-integer instance value."""
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    with self.assertRaisesRegex(
        modules.InvalidInstancesError,
        'Instance must be a non-negative integer.'):
      modules.get_hostname(instance='foo')

  def testGetHostname_NoVersion_VersionExistsOnTarget(self):
    """Tests no-version call where the current version exists on the target."""
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_hostname').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_modules')
    modules.get_modules().AndReturn(['default', 'module1'])
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')
    self.mox.StubOutWithMock(modules, 'get_versions')
    modules.get_versions(module='module1').AndReturn(['v1', 'v2'])
    mock_apps = self.mox.CreateMockAnything()
    mock_get_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.get(appsId='project').AndReturn(mock_get_request)
    mock_get_request.execute().AndReturn(
        {'defaultHostname': 'project.appspot.com'})
    self.mox.ReplayAll()
    self.assertEqual('v1.module1.project.appspot.com',
                     modules.get_hostname(module='module1'))

  def testGetHostname_NoVersion_VersionDoesNotExistOnTarget(self):
    """Tests no-version call where the current version is not on the target."""
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_hostname').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_modules')
    modules.get_modules().AndReturn(['default', 'module1'])
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')
    self.mox.StubOutWithMock(modules, 'get_versions')
    modules.get_versions(module='module1').AndReturn(['v2', 'v3'])
    mock_apps = self.mox.CreateMockAnything()
    mock_get_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.get(appsId='project').AndReturn(mock_get_request)
    mock_get_request.execute().AndReturn(
        {'defaultHostname': 'project.appspot.com'})
    self.mox.ReplayAll()
    self.assertEqual('module1.project.appspot.com',
                     modules.get_hostname(module='module1'))

  def testGetHostname_LegacyApp_Success(self):
    """Tests a hostname request for a legacy app without engines."""
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    mock_client = self.mox.CreateMockAnything()
    self.mox.StubOutWithMock(modules, '_get_admin_api_client_with_useragent')
    modules._get_admin_api_client_with_useragent('get_hostname').AndReturn(mock_client)
    self.mox.StubOutWithMock(modules, '_get_project_id')
    modules._get_project_id().AndReturn('project')
    self.mox.StubOutWithMock(modules, 'get_modules')
    modules.get_modules().AndReturn(['default'])
    self.mox.StubOutWithMock(modules, 'get_current_module_name')
    modules.get_current_module_name().AndReturn('default')
    self.mox.StubOutWithMock(modules, 'get_current_version_name')
    modules.get_current_version_name().AndReturn('v1')
    mock_apps = self.mox.CreateMockAnything()
    mock_get_request = self.mox.CreateMockAnything()
    mock_client.apps().AndReturn(mock_apps)
    mock_apps.get(appsId='project').AndReturn(mock_get_request)
    mock_get_request.execute().AndReturn(
        {'defaultHostname': 'project.appspot.com'})
    self.mox.ReplayAll()
    self.assertEqual('v1.project.appspot.com', modules.get_hostname())

  def testGetHostname_LegacyApp_WithInstance(self):
    """Tests a legacy app request with an invalid non-integer instance."""
    os.environ['MODULES_USE_ADMIN_API'] = 'true'
    with self.assertRaisesRegex(
        modules.InvalidInstancesError,
        'Instance must be a non-negative integer.'):
      modules.get_hostname(instance='i')
      
   # --- Tests for Legacy get_hostname ---

  def testGetHostnameLegacy(self):
    """Test we pass through the expected args."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    expected_request.instance = '3'
    service_response = modules_service_pb2.GetHostnameResponse()
    service_response.hostname = 'abc'
    self.SetSuccessExpectations('GetHostname',
                                expected_request,
                                service_response)
    self.assertEqual('abc', modules.get_hostname('module1', 'v1', '3'))

  def testGetHostnameLegacy_NoModule(self):
    """Test we pass through the expected args when no module is specified."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.version = 'v1'
    expected_request.instance = '3'
    service_response = modules_service_pb2.GetHostnameResponse()
    service_response.hostname = 'abc'
    self.SetSuccessExpectations('GetHostname',
                                expected_request,
                                service_response)
    self.assertEqual('abc', modules.get_hostname(version='v1', instance='3'))

  def testGetHostnameLegacy_NoVersion(self):
    """Test we pass through the expected args when no version is specified."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.module = 'module1'
    expected_request.instance = '3'
    service_response = modules_service_pb2.GetHostnameResponse()
    service_response.hostname = 'abc'
    self.SetSuccessExpectations('GetHostname',
                                expected_request,
                                service_response)
    self.assertEqual('abc',
                     modules.get_hostname(module='module1', instance='3'))

  def testGetHostnameLegacy_IntInstance(self):
    """Test we pass through the expected args when an int instance is given."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.module = 'module1'
    expected_request.instance = '3'
    service_response = modules_service_pb2.GetHostnameResponse()
    service_response.hostname = 'abc'
    self.SetSuccessExpectations('GetHostname',
                                expected_request,
                                service_response)
    self.assertEqual('abc', modules.get_hostname(module='module1', instance=3))

  def testGetHostnameLegacy_InstanceZero(self):
    """Test we pass through the expected args when instance zero is given."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.module = 'module1'
    expected_request.instance = '0'
    service_response = modules_service_pb2.GetHostnameResponse()
    service_response.hostname = 'abc'
    self.SetSuccessExpectations('GetHostname',
                                expected_request,
                                service_response)
    self.assertEqual('abc', modules.get_hostname(module='module1', instance=0))

  def testGetHostnameLegacy_NoArgs(self):
    """Test we pass through the expected args when none are given."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    service_response = modules_service_pb2.GetHostnameResponse()
    service_response.hostname = 'abc'
    self.SetSuccessExpectations('GetHostname',
                                expected_request,
                                service_response)
    self.assertEqual('abc', modules.get_hostname())

  def testGetHostnameLegacy_BadInstanceType(self):
    """Test get_hostname throws a TypeError when passed a float for instance."""
    self.assertRaises(TypeError,
                      modules.get_hostname,
                      'module1',
                      'v1',
                      1.2)

  def testGetHostnameLegacy_InvalidModuleError(self):
    """Test we raise an error when we receive one from the API."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.SetExceptionExpectations(
        'GetHostname', expected_request,
        modules_service_pb2.ModulesServiceError.INVALID_MODULE)
    self.assertRaises(modules.InvalidModuleError,
                      modules.get_hostname,
                      'module1',
                      'v1')

  def testGetHostnameLegacy_InvalidInstancesError(self):
    """Test we raise an error when we receive one from the API."""
    self.SetExceptionExpectations(
        'GetHostname', modules_service_pb2.GetHostnameRequest(),
        modules_service_pb2.ModulesServiceError.INVALID_INSTANCES)
    self.assertRaises(modules.InvalidInstancesError, modules.get_hostname)

  def testGetHostnameLegacy_UnKnownError(self):
    """Test we raise an error when we receive one from the API."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.SetExceptionExpectations(
        'GetHostname', expected_request, 1099)
    self.assertRaisesRegex(modules.Error,
                           'ApplicationError: 1099',
                           modules.get_hostname,
                           'module1',
                           'v1')

  def testGetHostnameLegacy_UnMappedError(self):
    """Test we raise an error when we receive one from the API."""
    expected_request = modules_service_pb2.GetHostnameRequest()
    expected_request.module = 'module1'
    expected_request.version = 'v1'
    self.SetExceptionExpectations(
        'GetHostname', expected_request,
        modules_service_pb2.ModulesServiceError.INVALID_VERSION)
    expected_message = 'ApplicationError: %s' % (
        modules_service_pb2.ModulesServiceError.INVALID_VERSION)
    self.assertRaisesRegex(modules.Error,
                           expected_message,
                           modules.get_hostname,
                           'module1',
                           'v1')

class MockRpc(object):
  """Mock UserRPC class."""

  def __init__(self, expected_method, expected_request, service_response=None,
               application_error_number=None):
    self._expected_method = expected_method
    self._expected_request = expected_request
    self._service_response = service_response
    self._application_error_number = application_error_number

  def check_success(self):
    self._check_success_called = True
    if self._application_error_number is not None:
      raise apiproxy_errors.ApplicationError(self._application_error_number)
    self.response.CopyFrom(self._service_response)

  def get_result(self):
    self._check_success_called = False
    result = self._hook(self)
    if not self._check_success_called:
      raise AssertionError('The hook is expected to call check_success()')
    return result

  def make_call(self, method,
                request, response, get_result_hook=None, user_data=None):
    self.method = method
    if self._expected_method != method:
      raise ValueError('expected method %s but got method %s' %
                       (self._expected_method, method))
    self.request = request
    if self._expected_request != request:
      raise ValueError('expected request %s but got request %s' %
                       (self._expected_request, request))
    self.response = response
    self._hook = get_result_hook
    self.user_data = user_data

if __name__ == '__main__':
  absltest.main()

