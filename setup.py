import setuptools

setuptools.setup(
  name='modules',
  version='0.0.1',
  install_requires=[
    'apache-beam[gcp]==2.53.0',
    'opencv-python==4.9.0.80',
  ],
  packages=setuptools.find_packages()
)

setuptools.setup(
  name='options',
  version='0.0.1',
  install_requires=['apache-beam==2.53.0'],
  packages=setuptools.find_packages()
)