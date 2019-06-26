from copy import deepcopy
from os import environ
from pprint import pprint
import yaml
from kubernetes import client, config
from kubernetes.client.rest import ApiException

_mpijob_template = {
 'apiVersion': 'kubeflow.org/v1alpha1',
 'kind': 'MPIJob',
 'metadata': {
     'name': '',
     'namespace': 'default-tenant'
 },
 'spec': {
     'replicas': 1,
     'template': {
         'spec': {
             'containers': [{
                 'image': 'gshatz/horovod:0.1.0',
                 'name': '',
                 'command': [],
                 'volumeMounts': [],
                 'securityContext': {
                     'capabilities': {'add': ['IPC_LOCK']}},
                 'resources': {
                     'limits': {'nvidia.com/gpu': 1}}}],
             'volumes': []
         }}}}


class MpiJob:
    """
    A wrapper over Kubernetes MPIJob (Horovod).

    Example:

       from mpijob import MpiJob

       job = MpiJob('myname', 'img', ['a','b'])
       job.volume()   # add v3io volume
       print(job.to_yaml())
       job.submit()

    """
    group = 'kubeflow.org'
    version = 'v1alpha1'
    plural = 'mpijobs'

    def __init__(self, name, image=None, command=None,
                 replicas=0, namespace='default-tenant'):
        self.api_instance = None
        self.name = name
        self.namespace = namespace
        self._struct = deepcopy(_mpijob_template)
        self._struct['metadata'] = {'name': name, 'namespace': namespace}
        self._update_container('name', name)
        if image:
            self._update_container('image', image)
        if command:
            self._update_container('command', ['mpirun','python'] + command)
        if replicas:
            self._struct['spec']['replicas'] = replicas

    def _update_container(self, key, value):
        self._struct['spec']['template']['spec']['containers'][0][key] = value

    def volume(self, mount='/User', volpath='~/', access_key=''):
        self._update_container('volumeMounts', [{'name': 'v3io', 'mountPath': mount}])

        if volpath.startswith('~/'):
            user = environ.get('V3IO_USERNAME', '')
            volpath = 'users/' + user + volpath[1:]

        container, subpath = split_path(volpath)
        access_key = access_key or environ.get('V3IO_ACCESS_KEY','')

        vol = {'name': 'v3io', 'flexVolume': {
            'driver': 'v3io/fuse',
            'options': {
                'container': container,
                'subPath': subpath,
                'accessKey': access_key,
            }
        }}

        self._struct['spec']['template']['spec']['volumes'] = [vol]
        return self

    def gpus(self, num, gpu_type='nvidia.com/gpu'):
        self._update_container('resources', {'limits' : {gpu_type: num}})
        return self

    def replicas(self, replicas_num):
        self._struct['spec']['replicas'] = replicas_num
        return self

    def to_dict(self):
        return self._struct

    def to_yaml(self):
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    def submit(self):
        config.load_incluster_config()
        self.api_instance = client.CustomObjectsApi()

        try:
            api_response = self.api_instance.create_namespaced_custom_object(
                MpiJob.group, MpiJob.version, self.namespace, 'mpijobs', self.to_dict())
            pprint(api_response)
        except ApiException as e:
            print("Exception when creating MPIJob: %s" % e)

    def delete(self):
        try:
            # delete the mpi job\
            body = client.V1DeleteOptions()
            api_response = self.api_instance.delete_namespaced_custom_object(
                MpiJob.group, MpiJob.version, self.namespace, MpiJob.plural, self.name, body)
            pprint(api_response)
        except ApiException as e:
            print("Exception when calling CustomObjectsApi->delete_namespaced_custom_object: %s\\n" % e)

def split_path(mntpath=''):
    if mntpath[0] == '/':
        mntpath = mntpath[1:]
    paths = mntpath.split('/')
    container = paths[0]
    subpath = ''
    if len(paths) > 1:
        subpath = mntpath[len(container):]
    return container, subpath