import os
import logging
import globus_sdk
from globus_sdk.token_storage import JSONTokenStorage

# Configure a module-level logger
logger = logging.getLogger("sdss_transfer.globus")

class Globus_cli:
    """
    A class to manage synchronous Globus data transfers between the 
    SDSS Admin Collection and the JHU IDIES endpoint, featuring automated 
    token caching to bypass repeated Utah 2FA logins.
    """
    def __init__(self):
        # 1. Load configuration from system environment variables
        self.client_id = os.environ.get("TRANSFER_CLIENT_ID")
        self.source_endpoint = os.environ.get("TRANSFER_SAS_ENDPOINT")
        self.destination_endpoint = os.environ.get("TRANSFER_SAM_ENDPOINT")
        self.product_directory = "/uufs/chpc.utah.edu/common/home/sdssadmin/test"
        
        # 2. Set up the secure local token storage cache
        self.token_file_path = os.path.expanduser("~/.globus/globus-auth.json")
        self.token_storage = JSONTokenStorage(self.token_file_path)
        
        # 3. Validate environment setup immediately on instantiation
        self._validate_environment()
        
        # 4. Initialize the foundational Native Application Auth Client
        self.auth_client = globus_sdk.NativeAppAuthClient(self.client_id)

    def _validate_environment(self):
        """Internal helper to ensure all necessary environment variables exist."""
        missing_variables = []
        if not self.client_id: missing_variables.append("GLOBUS_CLIENT_ID")
        if not self.source_endpoint: missing_variables.append("SDSS_ADMIN_COLLECTION_UUID")
        if not self.destination_endpoint: missing_variables.append("JHU_IDIES_ENDPOINT_UUID")
        if not self.product_directory: missing_variables.append("PRODUCT_DIR")
        
        if missing_variables:
            error_message = f"Missing required environment variables: {', '.join(missing_variables)}"
            logger.critical(error_message)
            raise EnvironmentError(error_message)

    def _ensure_authenticated(self):
        """
        Validates cached tokens or prompts for a one-time 2FA login.
        Returns authorizers for both Transfer and Auth API operations.
        """
        # Attempt to load existing credentials from disk
        transfer_token_data = self.token_storage.get_token_data("transfer.api.globus.org")
        auth_token_data = self.token_storage.get_token_data("auth.globus.org")

        if not transfer_token_data or not auth_token_data:
            # We explicitly request refresh_tokens=True to survive token expiration
            self.auth_client.oauth2_start_flow(refresh_tokens=True)
            
            print("\n[Globus Auth] No cached tokens found. Initializing secure one-time authentication.")
            print(f"Please log in here (requires Utah 2FA):\n{self.auth_client.oauth2_get_authorize_url()}\n")
            
            authorization_code = input("Enter the resulting authorization code: ").strip()
            token_response = self.auth_client.oauth2_exchange_code_for_tokens(authorization_code)
            
            # Save the tokens securely to ~/.sdss_transfer_tokens.json
            self.token_storage.store_token_response(token_response)
            print("[Globus Auth] Tokens successfully cached! You will not need to do this step again.\n")
            
            # Reload the data from our newly written cache
            transfer_token_data = self.token_storage.get_token_data("transfer.api.globus.org")
            auth_token_data = self.token_storage.get_token_data("auth.globus.org")

        # Create Transfer Authorizer (automatically saves to disk when tokens refresh)
        transfer_authorizer = globus_sdk.RefreshTokenAuthorizer(
            transfer_token_data.refresh_token, 
            self.auth_client, 
            access_token=transfer_token_data.access_token, 
            expires_at=transfer_token_data.expires_at_seconds,
            on_refresh=self.token_storage.store_token_response
        )

        # Create Auth Authorizer (needed for identity lookups like 'whoami')
        auth_authorizer = globus_sdk.RefreshTokenAuthorizer(
            auth_token_data.refresh_token, 
            self.auth_client, 
            access_token=auth_token_data.access_token, 
            expires_at=auth_token_data.expires_at_seconds,
            on_refresh=self.token_storage.store_token_response
        )

        return transfer_authorizer, auth_authorizer

    def whoami(self):
        """
        Retrieves and prints the active user's identity details using 
        the cached tokens, mimicking the `globus whoami` CLI command.
        """
        _, auth_authorizer = self._ensure_authenticated()
        
        # Create a new AuthClient specifically bound to the user's authorizer
        bound_auth_client = globus_sdk.AuthClient(authorizer=auth_authorizer)
        
        try:
            user_profile = bound_auth_client.userinfo()
            print("\n--- GLOBUS WHOAMI ---")
            print(f"Username: {user_profile.get('username')}")
            print(f"Name:     {user_profile.get('name')}")
            print(f"Email:    {user_profile.get('email')}")
            print(f"ID:       {user_profile.get('sub')}")
            print("---------------------\n")
            return user_profile
        except Exception as error:
            logger.error(f"Failed to fetch user info: {str(error)}")
            return None

    def _is_endpoint_accessible(self, transfer_client, endpoint_id, label="Endpoint"):
        """
        Validates if an endpoint/collection is online and responsive.
        Aborts early if the target is down for maintenance or offline.
        """
        try:
            endpoint_information = transfer_client.get_endpoint(endpoint_id)
            if endpoint_information.get("non_functional") is True:
                logger.error(f"HEALTH CHECK FAILED: {label} ({endpoint_id}) is marked NON-FUNCTIONAL.")
                return False
                
            if endpoint_information.get("entity_type") == "GCP_mapped_collection" or "gcp_connected" in endpoint_information:
                if not endpoint_information.get("gcp_connected", True):
                    logger.error(f"HEALTH CHECK FAILED: Globus Connect Personal {label} ({endpoint_id}) is offline.")
                    return False

            test_path = endpoint_information.get("default_directory") or "/"
            transfer_client.operation_ls(endpoint_id, path=test_path, limit=1)
            
            logger.info(f"HEALTH CHECK PASSED: Verified live connectivity to {label} ({endpoint_id}).")
            return True

        except globus_sdk.TransferAPIError as error:
            if error.code in ["PermissionDenied", "ConsentRequired", "AuthenticationFailed"]:
                logger.info(f"HEALTH CHECK PASSED: Verified live connectivity to {label} ({endpoint_id}) [Status: {error.code}].")
                return True
            
            logger.error(f"HEALTH CHECK FAILED: {label} ({endpoint_id}) is unreachable. Code: {error.code} - {error.message}")
            return False
        except Exception as error:
            logger.error(f"Unexpected error when checking health for {label}: {str(error)}")
            return False

    def execute_transfer(self, keyword, destination_directory, label=None):
        """
        Validates both endpoints, dynamically constructs paths, 
        and executes the transfer securely, blocking until completion.
        """
        source_path = os.path.join(self.product_directory, keyword)
        transfer_label = label or f"SDSS to JHU: {keyword}"
        
        # Load tokens from cache or prompt for login
        transfer_authorizer, _ = self._ensure_authenticated()
        transfer_client = globus_sdk.TransferClient(authorizer=transfer_authorizer)
        
        logger.info("Initiating pre-transfer endpoint validation...")
        
        if not self._is_endpoint_accessible(transfer_client, self.source_endpoint, "Source (SDSS Admin)"):
            logger.critical("CRITICAL: Source endpoint is down or under maintenance. Transfer aborted.")
            return False
            
        if not self._is_endpoint_accessible(transfer_client, self.destination_endpoint, "Destination (JHU IDIES)"):
            logger.critical("CRITICAL: Destination endpoint is down or under maintenance. Transfer aborted.")
            return False

        logger.info("Both endpoints are online. Building transfer dataset...")
        transfer_data = globus_sdk.TransferData(
            transfer_client, 
            self.source_endpoint, 
            self.destination_endpoint, 
            label=transfer_label, 
            sync_level="checksum"
        )
        transfer_data.add_item(source_path, destination_directory, recursive=True)
        
        try:
            logger.info(f"Submitting transfer from {source_path} to {destination_directory}...")
            submit_result = transfer_client.submit_transfer(transfer_data)
            task_id = submit_result["task_id"]
            logger.info(f"Transfer submitted successfully. Task ID: {task_id}")
            
            logger.info("Waiting for transfer execution...")
            transfer_client.task_wait(task_id, timeout=86400, polling_interval=10)
            
            task = transfer_client.get_task(task_id)
            status = task["status"]
            
            if status == "SUCCEEDED":
                logger.info(f"SUCCESS: Transfer task {task_id} completed smoothly.")
                return True
            elif status == "FAILED":
                error_message = task.get("fatal_error", "Unknown fatal error occurred.")
                logger.error(f"FAILURE: Transfer task {task_id} failed. Reason: {error_message}")
                return False
            else:
                logger.warning(f"WARNING: Transfer task {task_id} finished with unexpected status: {status}")
                return False
                
        except globus_sdk.TransferAPIError as error:
            logger.error(f"Globus Transfer API Error: {error.http_status} - {error.code} - {error.message}")
            raise
        except Exception as error:
            logger.error(f"Unexpected error during transfer lifecycle: {str(error)}")
            raise
