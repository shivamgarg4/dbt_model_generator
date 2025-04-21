"""
DBT Model Generator Package

A tool for generating and managing DBT models and related components.
"""

from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution('dbt_model_generator').version
except DistributionNotFound:
    __version__ = 'unknown'
