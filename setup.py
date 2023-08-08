from setuptools import setup

setup(
    name='My_own_package',
    version='0.0.1',
    description='My private package from private github repo',
    url='git@github.com:intelspeedstep/My_own_package.git',
    author='David Geng',
    author_email='david.geng@valpo.edu',
    license='unlicense',
    packages=['My_own_package'],
    zip_safe=False
)