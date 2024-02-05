import setuptools

setuptools.setup(
  name='modules',
  version='0.0.1',
  install_requires=[
    'apache-beam[gcp]==2.53.0',
    'opencv-python-headless==4.9.0.80',
  ],
  package_dir={
    "main": ".",
    "modules": "modules"
  },
  packages=["main", "modules"]
)