import argparse
import pytest

from wintappy.config import EnvironmentConfig

pytestmark = pytest.mark.unit

class TestEnvironmentConfig:
    
    def _get_parser(self):
        return argparse.ArgumentParser(
            description="Test argparse parser",
        )

    def test_default_args(self):
        parser = self._get_parser()
        env_config = EnvironmentConfig(parser)
        options = env_config.get_options([])
        assert options.LOG_LEVEL == 'INFO'
        for v in ['DATASET', 'AGGLEVEL', 'START', 'END', 'AWS_PROFILE']:
            assert v not in options.keys()
        
    def test_start_end_default(self):
        parser = self._get_parser()
        env_config = EnvironmentConfig(parser)
        env_config.add_start()
        env_config.add_end()
        options = env_config.get_options(['-s', '20240111', '-e', '20240112'])
        assert options.START == '20240111'
        assert options.END == '20240112'
        parser = self._get_parser()
    
    def test_start_end_required(self):
        parser = self._get_parser()
        env_config = EnvironmentConfig(parser)
        env_config.add_start(required=True)
        env_config.add_end(required=True)
        options = env_config.get_options(['-s', '20240111', '-e', '20240112'])
        assert options.START == '20240111'
        assert options.END == '20240112'
        with pytest.raises(SystemExit):
            # missing start 
            _ = env_config.get_options(['-e', '20240112'])
        with pytest.raises(SystemExit):
            # missing end 
            _ = env_config.get_options(['-s', '20240112'])

    def test_aws_settings(self):
        parser = self._get_parser()
        env_config = EnvironmentConfig(parser)
        env_config.add_aws_settings()
        options = env_config.get_options(['-b', 'my-bucket', '--aws-profile', 'my-profile', '-p', 'my-prefix'])
        assert options.AWS_S3_BUCKET == 'my-bucket'
        assert options.AWS_S3_PREFIX == 'my-prefix'
        assert options.AWS_PROFILE == 'my-profile'
        assert options.LOG_LEVEL == 'INFO'
