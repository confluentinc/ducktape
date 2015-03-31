from setuptools import find_packages, setup

setup(name="ducktape",
      version="0.1",
      description="Distributed system test tools",
      author="Ewen Cheslack-Postava",
      author_email='ewen@confluent.io',
      platforms=["any"], 
      license="apache2.0",
      url="http://github.com/confluentinc/ducktape",
      packages=find_packages(),
      )
