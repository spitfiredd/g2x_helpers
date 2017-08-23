from setuptools import setup

setup(name='g2x_helpers',
      version='0.1',
      description='Helper Functions used across different packages',
      author='Daniel Donovan',
      author_email='spitfiredd@gmail.com',
      url='https://github.com/spitfiredd/g2x_helpers',
      license='MIT',
      packages=['g2x_helpers', 'g2x_helpers.pysam', 'g2x_helpers.pyfpds'],
      zip_safe=False)
