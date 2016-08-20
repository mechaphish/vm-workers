from distutils.core import setup

setup(
      name='vm-worker',
      version='0.01',
      packages=['test_vm_worker'],
      scripts=['bin/common_tester'],
      description='Repo that contains components that need to run inside CGC VM.',
      url='https://github.com/mechaphish/vm-workers',
)
