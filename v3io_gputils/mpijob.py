from copy import deepcopy
from os import environ
from pprint import pprint
import yaml
from kubernetes import client, config
from kubernetes.client.rest import ApiException


class MPIJobPodTemplateType(object):
    launcher = 'launcher'
    worker = 'worker'

    @staticmethod
    def all():
        return [
            MPIJobPodTemplateType.launcher,
            MPIJobPodTemplateType.worker
        ]


_mpijob_launcher_pod_template = {
    'spec': {
        'containers': [
            {
                'image': 'iguaziodocker/horovod:0.1.1',
                'name': '',
                'command': [],
                'workingDir': '/User',
                'securityContext': {
                    'capabilities': {'add': ['IPC_LOCK']}
                }
            }
        ],
        'volumes': [{
            'name': 'v3io',
            'flexVolume': {
                'driver': 'v3io/fuse',
                'options': {
                    'container': 'users',
                    'subPath': '',
                    'accessKey': '',
                }
            }}]
    },
}

_mpijob_worker_pod_template = {
    'spec': {
        'containers': [
            {
                'image': 'iguaziodocker/horovod:0.1.1',
                'name': '',
                'command': [],
                'volumeMounts': [{'name': 'v3io', 'mountPath': '/User'}],
                'workingDir': '/User',
                'securityContext': {
                    'capabilities': {'add': ['IPC_LOCK']}},
                'resources': {
                    'limits': {'nvidia.com/gpu': 1}}}],
        'volumes': [{
            'name': 'v3io',
            'flexVolume': {
                'driver': 'v3io/fuse',
                'options': {
                    'container': 'users',
                    'subPath': '',
                    'accessKey': '',
                }
            }}]
    },
}


class MpiJob:
    """
    A wrapper over Kubernetes MPIJob (Horovod).

    Example:

       from mpijob import MpiJob

       job = MpiJob('myname', 'img', ['a','b'])
       print(job.to_yaml())
       job.submit()

    """
    group = 'kubeflow.org'
    version = 'v1'
    plural = 'mpijobs'

    def __init__(self, name, image=None, command=None,
                 replicas=1, namespace='default-tenant'):
        self.api_instance = None
        self.name = name
        self.namespace = namespace

        self._pod_templates = {
            MPIJobPodTemplateType.launcher: deepcopy(_mpijob_launcher_pod_template),
            MPIJobPodTemplateType.worker: deepcopy(_mpijob_worker_pod_template)
        }

        self._update_container('name', name, MPIJobPodTemplateType.all())
        if image:
            self._update_container('image', image, MPIJobPodTemplateType.all())
        if command:
            self._update_container('command', ['mpirun', 'python'] + command, [MPIJobPodTemplateType.worker])

        self._update_access_token(environ.get('V3IO_ACCESS_KEY', ''), MPIJobPodTemplateType.all())
        self._update_running_user(environ.get('V3IO_USERNAME', ''), MPIJobPodTemplateType.all())

        self._struct = self._generate_mpi_job_template(name, namespace, replicas)

    def _generate_mpi_job_template(self, name, namespace, worker_replicas):
        return {
            'apiVersion': 'kubeflow.org/v1',
            'kind': 'MPIJob',
            'metadata': {'name': name, 'namespace': namespace},
            'spec': {
                'slotsPerWorker': 1,
                'mpiReplicaSpecs': {
                    'Launcher': {
                        'template': self._pod_templates[MPIJobPodTemplateType.launcher]
                    },
                    'Worker': {
                        'replicas': worker_replicas,
                        'template': self._pod_templates[MPIJobPodTemplateType.launcher]
                    },
                },
            },
        }

    def _update_container(self, key, value, template_types):
        for template_type in template_types:
            self._pod_templates[template_type]['spec']['containers'][0][key] = value

    def _update_access_token(self, token, template_types):
        for template_type in template_types:
            self._pod_templates[template_type]['spec']['volumes'][0]['flexVolume']['options']['accessKey'] = token

    def _update_running_user(self, username, template_types):
        for template_type in template_types:
            self._pod_templates[template_type]['spec']['volumes'][0]['flexVolume']['options']['subPath'] = \
                '/' + username

    def _update_volumes(self, volumes, template_types):
        for template_type in template_types:
            self._pod_templates[template_type]['volumes'] = volumes

    def volume(self, mount='/User', volpath='~/', access_key=''):
        self._update_container('volumeMounts', [{'name': 'v3io', 'mountPath': mount}], MPIJobPodTemplateType.all())

        if volpath.startswith('~/'):
            v3io_home = environ.get('V3IO_HOME', '')
            volpath = v3io_home + volpath[1:]

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

        self._update_volumes([vol], MPIJobPodTemplateType.all())
        return self

    def gpus(self, num, gpu_type='nvidia.com/gpu'):
        self._update_container('resources', {'limits': {gpu_type: num}}, [MPIJobPodTemplateType.launcher])
        return self

    def replicas(self, replicas_num):
        self._struct['spec']['replicas'] = replicas_num
        return self

    def working_dir(self, working_dir):
        self._update_container('workingDir', working_dir, MPIJobPodTemplateType.all())
        return self

    def to_dict(self):
        return self._struct

    def to_yaml(self):
        noalias_dumper = yaml.dumper.SafeDumper
        noalias_dumper.ignore_aliases = lambda _self, data: True

        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False, Dumper=noalias_dumper)

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
            # delete the mpi job
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
