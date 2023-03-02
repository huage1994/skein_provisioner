from setuptools import setup, find_packages

setup(
    name="skein-provisioner",
    version="0.0.1",
    auth="Guanhua Li",
    packages=find_packages(),
    install_requires=[
        'jupyter_client>=7.0.0a1',
        'skein',
        'ipykernel',
        'tornado',
        'traitlets'
    ],

    entry_points={
        'jupyter_client.kernel_provisioners': [
            'skein-provisioner = skein_provisioner:SkeinProvisoner',
        ]
    },
)