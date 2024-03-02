import os
import subprocess

from setuptools import find_packages, setup

src_dir = os.path.dirname(__file__)
home_dir = os.path.abspath(os.path.join(src_dir, "../.."))

install_requires = [
    "six>=1.9.0",
    "boto3>1.12",
    "globus-sdk>=3.5.0;python_version>='3.6'",
    "google_api_python_client==2.120.0",
    "google_auth_oauthlib==0.4.6",
    "mlflow==2.11.0",
    "numpy==1.23.5",
    "pandas==1.5.1",
    "protobuf==3.20.0",
    "psutil==5.9.8",
    "PyGithub==2.2.0",
    "PyYAML==6.0",
    "PyYAML==6.0.1",
    "Requests==2.31.0"
]


#
# Utility function to read the pegasus Version.in file
#
def read_version():
    return (
        subprocess.Popen(
            "%s/release-tools/getversion" % home_dir, stdout=subprocess.PIPE, shell=True
        )
        .communicate()[0]
        .decode()
        .strip()
    )


#
# Utility function to read the README file.
#
def read(fname):
    return open(os.path.join(src_dir, fname)).read()


setup(
    name="pegasus-wms.worker",
    version=read_version(),
    author="Pegasus Team",
    author_email="pegasus@isi.edu",
    description="Pegasus Workflow Management System Worker Package Tools",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    license="Apache2",
    url="http://pegasus.isi.edu",
    project_urls={
        "Documentation": "https://pegasus.isi.edu/documentation/",
        "Changes": "https://pegasus.isi.edu/blog/?category_name=Release",
        "Source Code": "https://github.com/pegasus-isi/pegasus",
        "Issue Tracker": "https://jira.isi.edu/projects/PM/issues",
    },
    python_requires=">=2.6,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*",
    keywords=["scientific workflows"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
