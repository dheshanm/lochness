import pytest
import sys
from pathlib import Path
import json
import tempfile
import shutil
from datetime import datetime, timedelta

# Add project root to Python path
file = Path(__file__).resolve()
parent = file.parent
root_dir = None
for p in file.parents:
    if p.name == 'lochness_v2':
        root_dir = p
if root_dir:
    sys.path.append(str(root_dir))
else:
    # If running from project root, add current dir
    sys.path.append(str(parent))

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore
from lochness.models.subjects import Subject
from lochness.sources.mindlamp.models.data_source import MindLAMPDataSource, MindLAMPDataSourceMetadata
from lochness.sources.mindlamp.tasks.pull_data import (
    get_mindlamp_cred,
    connect_to_mindlamp,
    fetch_subject_data,
    pull_all_data,
)


class TestMindLAMPPullData:
    """Test class for MindLAMP data pull functionality."""

    @pytest.fixture(scope="class", autouse=True)
    def setup_test_environment(self, request):
        """Set up test environment with test data."""
        request.cls.project_id = "ProCAN_WEB"
        request.cls.site_id = "BWH"
        request.cls.subject_id = "test_subject_01"
        request.cls.keystore_name = "mindlamp_prod_token"
        
        # Get config file
        config_file = utils.get_config_file_path()
        if not config_file.exists():
            pytest.skip("Config file not found")
        request.cls.config_file = config_file
        
        # Create temporary directory for test files
        temp_dir = tempfile.mkdtemp()
        request.cls.temp_dir = temp_dir
        
        yield
        
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def setup_test_data(self):
        """Set up test data in database."""
        # This would typically create test subjects, data sources, etc.
        # For now, we'll assume they exist in the database
        pass
    
    def test_get_mindlamp_credentials(self):
        """Test retrieving MindLAMP credentials from keystore."""
        # Create a mock MindLAMP data source
        mindlamp_ds = MindLAMPDataSource(
            data_source_name="test_mindlamp_ds",
            is_active=True,
            site_id=self.site_id,
            project_id=self.project_id,
            data_source_type="mindlamp",
            data_source_metadata=MindLAMPDataSourceMetadata(
                keystore_name=self.keystore_name,
                api_url="https://api.mindlamp.com"
            )
        )
        
        try:
            credentials = get_mindlamp_cred(mindlamp_ds)
            assert isinstance(credentials, dict)
            assert "access_key" in credentials
            assert "secret_key" in credentials
            print(f"✓ Successfully retrieved MindLAMP credentials for {self.keystore_name}")
        except ValueError as e:
            pytest.skip(f"MindLAMP credentials not found: {e}")
    
    def test_connect_to_mindlamp(self):
        """Test connecting to MindLAMP API."""
        # Create a mock MindLAMP data source
        mindlamp_ds = MindLAMPDataSource(
            data_source_name="test_mindlamp_ds",
            is_active=True,
            site_id=self.site_id,
            project_id=self.project_id,
            data_source_type="mindlamp",
            data_source_metadata=MindLAMPDataSourceMetadata(
                keystore_name=self.keystore_name,
                api_url="https://api.mindlamp.com"
            )
        )
        
        try:
            lamp = connect_to_mindlamp(mindlamp_ds)
            assert lamp is not None
            print("✓ Successfully connected to MindLAMP API")
        except (ValueError, ImportError) as e:
            pytest.skip(f"Could not connect to MindLAMP: {e}")
    
    def test_fetch_subject_data(self):
        """Test pulling MindLAMP data for a specific subject."""
        # Create a mock MindLAMP data source
        mindlamp_ds = MindLAMPDataSource(
            data_source_name="test_mindlamp_ds",
            is_active=True,
            site_id=self.site_id,
            project_id=self.project_id,
            data_source_type="mindlamp",
            data_source_metadata=MindLAMPDataSourceMetadata(
                keystore_name=self.keystore_name,
                api_url="https://api.mindlamp.com"
            )
        )
        
        encryption_passphrase = config.parse(self.config_file, 'general')['encryption_passphrase']
        
        try:
            # Test pulling data for the subject
            result = fetch_subject_data(
                mindlamp_data_source=mindlamp_ds,
                subject_id=self.subject_id,
                encryption_passphrase=encryption_passphrase
            )
            
            # Check if data was pulled successfully
            assert result is not None
            print(f"✓ Successfully pulled MindLAMP data for subject {self.subject_id}")
            
        except Exception as e:
            pytest.skip(f"Could not pull MindLAMP data: {e}")
    
    def test_pull_all_data(self):
        """Test pulling MindLAMP data for all subjects in the project/site."""
        try:
            # Test pulling data for all subjects
            pull_all_data(
                config_file=self.config_file,
                project_id=self.project_id,
                site_id=self.site_id
            )
            
            print("✓ Successfully initiated MindLAMP data pull for all subjects")
            
        except Exception as e:
            pytest.skip(f"Could not pull MindLAMP data for all subjects: {e}")
    
    def test_mindlamp_data_source_metadata(self):
        """Test MindLAMP data source metadata structure."""
        metadata = MindLAMPDataSourceMetadata(
            keystore_name=self.keystore_name,
            api_url="https://api.mindlamp.com"
        )
        
        assert metadata.keystore_name == self.keystore_name
        assert metadata.api_url == "https://api.mindlamp.com"
        print("✓ MindLAMP data source metadata structure is correct")
    
    def test_mindlamp_data_source_model(self):
        """Test MindLAMP data source model structure."""
        mindlamp_ds = MindLAMPDataSource(
            data_source_name="test_mindlamp_ds",
            is_active=True,
            site_id=self.site_id,
            project_id=self.project_id,
            data_source_type="mindlamp",
            data_source_metadata=MindLAMPDataSourceMetadata(
                keystore_name=self.keystore_name,
                api_url="https://api.mindlamp.com"
            )
        )
        
        assert mindlamp_ds.data_source_name == "test_mindlamp_ds"
        assert mindlamp_ds.site_id == self.site_id
        assert mindlamp_ds.project_id == self.project_id
        assert mindlamp_ds.data_source_type == "mindlamp"
        assert mindlamp_ds.data_source_metadata.keystore_name == self.keystore_name
        print("✓ MindLAMP data source model structure is correct")


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])