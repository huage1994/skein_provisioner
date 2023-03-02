import asyncio
import getpass
import json
import os
from typing import List, Any, Optional, Dict

from jupyter_client import KernelProvisionerBase, KernelConnectionInfo
from skein import ApplicationSpec, Resources, ApplicationNotRunningError
from skein.model import ApplicationState, Master

from skein_provisioner.skein_driver import SkeinDriverProvider

default_kernel_launch_timeout = os.environ.get('SKEIN_POLL_TIMES', 30)

class SkeinProvisoner(KernelProvisionerBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.app_id = ''
        self.app = None
        self.driver_provider = SkeinDriverProvider()
        venv_path = os.environ.get('IPYTHON_VENV')
        venv_key = {"IPYTHON_VENV"}
        venv_envs = {k: v for k, v in os.environ.copy().items() if k in venv_key}
        venv_tar_file = 'environment.tar.gz'
        self.ipykernel = Master(resources=Resources(memory=2048, vcores=1),
                                files={'environment': venv_path},
                                env=venv_envs,
                                script=('source /etc/profile\n' +
                                        'source environment/bin/activate\n' +
                                        'python -m skein.recipes.ipython_kernel'))
        self.spec = ApplicationSpec(name='ipython-kernel',
                                    master=self.ipykernel)

    async def pre_launch(self, **kwargs: Any) -> Dict[str, Any]:
        cmd = self.kernel_spec.argv  # Build launch command, provide substitutions

        kwargs = await super().pre_launch(cmd=cmd, **kwargs)
        env = kwargs.get('env', {})
        return kwargs

    async def launch_kernel(self, cmd: List[str], **kwargs: Any) -> KernelConnectionInfo:
        client = self.driver_provider.get_skein_driver_client()

        self.app_id = client.submit(self.spec)
        error_message = ''
        for x in range(default_kernel_launch_timeout):
            await asyncio.sleep(1)
            try:
                self.log.info(f"[skein] try to connect {self.app_id}. {x + 1}th times")
                self.app = client.connect(self.app_id, wait=False)
                break
            except ApplicationNotRunningError as e:
                error_message = str(e)
                continue
        if not self.app:
            # kill掉
            client.kill_application(self.app_id)
            erro_msg = f"KernelID: '{self.kernel_id}', ApplicationID: '{self.application_id}' " \
                       f"{error_message}" \
                       ""
            raise RuntimeError(erro_msg)

        self.log.info(f"[skein] app is: {self.app}")
        self.log.info(f"[skein] app id is: {self.app_id}")
        info_block = self.app.kv.wait('ipython.kernel.info')

        info = json.loads(info_block)
        self.log.info(f"[skein] info is: {info}")

        return info

    @property
    def has_process(self) -> bool:
        return self.app != None

    async def poll(self) -> Optional[int]:
        result = 0
        client = self.driver_provider.get_skein_driver_client()
        report = client.application_report(self.app_id)
        self.log.info(f"[skein] app state is: {report}")
        if report.state in (
                ApplicationState.NEW, ApplicationState.RUNNING, ApplicationState.ACCEPTED, ApplicationState.SUBMITTED):
            return None
        return result

    async def wait(self) -> Optional[int]:
        pass

    async def send_signal(self, signum: int) -> None:
        pass

    async def kill(self, restart: bool = False) -> None:
        client = self.driver_provider.get_skein_driver_client()
        client.kill_application(self.app_id)

    async def terminate(self, restart: bool = False) -> None:
        client = self.driver_provider.get_skein_driver_client()
        client.kill_application(self.app_id)

    async def cleanup(self, restart: bool = False) -> None:
        pass
