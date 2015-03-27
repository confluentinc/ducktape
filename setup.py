from setuptools import find_packages, setup

setup(name="ducttape",
      version="0.1",
      description="Distributed system test tools",
      author="Ewen Cheslack-Postava",
      author_email='ewen@confluent.io',
      platforms=["any"], 
      license="apache2.0",
      url="http://github.com/confluentinc/ducttape",
      packages=find_packages(),
      )
