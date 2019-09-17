import setuptools
REQUIRED_PACKAGES = [
    'beam-nuggets',
    'psycopg2-binary'
]
PACKAGE_NAME = 'demo1_paquete'
PACKAGE_VERSION = '0.0.1'
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Proyecto demo',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)