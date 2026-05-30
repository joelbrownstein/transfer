import os
import logging
import globus_sdk
from globus_sdk.token_storage import JSONTokenStorage
from globus_sdk.scopes import GCSCollectionScopes, TransferScopes

class Globus_cli:
    """
    A class to manage synchronous Globus data transfers between the 
    SDSS Admin Collection and the JHU IDIES endpoint, featuring automated 
    token caching to bypass repeated Utah 2FA logins. Fully optimized for SDK v4.
    """
    
    endpoints = ['source', 'destination']

    def __init__(self, logger = None, verbose = None):
        self.logger = logger if logger else logging.getLogger("sdss_transfer.globus")
        self.verbose = verbose
        self.client_id = os.environ.get("TRANSFER_CLIENT_ID")
        self.source_endpoint = os.environ.get("TRANSFER_SAS_ENDPOINT")
        self.destination_endpoint = os.environ.get("TRANSFER_SAM_ENDPOINT")
        self.set_ready()
        if self.ready:
            self.auth_client = globus_sdk.NativeAppAuthClient(self.client_id)
            self.set_token()
            self.set_client()
            self.set_endpoint()
        else: self.token = self.client = self.endpoint = None
        
    def set_endpoint(self):
        self.endpoint = {}
        for endpoint in self.endpoints: 
            self.set_endpoint_info(endpoint = endpoint)
            self.endpoint[endpoint] = self.endpoint_info
        self.ready = all(self.endpoint.values())

    def set_ready(self):
        """Internal helper to ensure all necessary environment variables exist."""
        missing_variables = []
        if not self.client_id: missing_variables.append("GLOBUS_CLIENT_ID")
        if not self.source_endpoint: missing_variables.append("SDSS_ADMIN_COLLECTION_UUID")
        if not self.destination_endpoint: missing_variables.append("JHU_IDIES_ENDPOINT_UUID")
        
        if missing_variables:
            error_message = f"Missing required environment variables: {', '.join(missing_variables)}"
            self.logger.critical(error_message)
            self.ready = False
        else: self.ready = True

    def set_client(self):
        authorizer = self.token['transfer_authorizer'] if self.token and 'transfer_authorizer' in self.token else None
        self.client = globus_sdk.TransferClient(authorizer=authorizer) if authorizer else None
        
    def set_token(self):
        """
        Validates cached tokens or prompts for a one-time 2FA login.
        Returns authorizers for both Transfer and Auth API operations.
        """
        
        self.token = {}
        self.token['file_path'] = os.path.expanduser("~/.globus/cli/globus-auth.json")
        self.token['file_exists'] = os.path.exists(self.token['file_path'])
        
        self.token['storage'] = JSONTokenStorage(self.token['file_path'])

        # Attempt to load existing credentials from disk
        if self.token['file_exists']:
            transfer_token_data = self.token['storage'].get_token_data("transfer.api.globus.org")
            auth_token_data = self.token['storage'].get_token_data("auth.globus.org")
        else: transfer_token_data = auth_token_data = None 

        if not transfer_token_data or not auth_token_data:
            source_scopes = GCSCollectionScopes(self.source_endpoint)
            dest_scopes = GCSCollectionScopes(self.destination_endpoint)
            transfer_scope = TransferScopes.all.with_dependencies([source_scopes.data_access, dest_scopes.data_access])
            requested_scopes = [ transfer_scope, "openid", "profile", "email" ]
            
            # Initialize the login flow with the defined scopes
            self.auth_client.oauth2_start_flow(requested_scopes=requested_scopes, refresh_tokens=True)
            authorize_url = self.auth_client.oauth2_get_authorize_url(session_required_single_domain="utah.edu")
            
            print("\n[Globus Auth] No cached tokens found. Initializing secure one-time authentication.")
            print(f"Please log in here (requires Utah 2FA):\n{authorize_url}\n")    
                    
            authorization_code = input("Enter the resulting authorization code: ").strip()
            token_response = self.auth_client.oauth2_exchange_code_for_tokens(authorization_code)
            
            self.token['storage'].store_token_response(token_response)
            print("[Globus Auth] Tokens successfully cached! You will not need to do this step again.\n")
            
            # Reload the data from our newly written cache
            transfer_token_data = self.token['storage'].get_token_data("transfer.api.globus.org")
            auth_token_data = self.token['storage'].get_token_data("auth.globus.org")

        # Create Transfer Authorizer (automatically saves to disk when tokens refresh)
        self.token['transfer_authorizer'] = globus_sdk.RefreshTokenAuthorizer(
            transfer_token_data.refresh_token, 
            self.auth_client, 
            access_token=transfer_token_data.access_token, 
            expires_at=transfer_token_data.expires_at_seconds,
            on_refresh=self.token['storage'].store_token_response
        )

        # Create Auth Authorizer (needed for identity lookups like 'whoami')
        self.token['auth_authorizer'] = globus_sdk.RefreshTokenAuthorizer(
            auth_token_data.refresh_token, 
            self.auth_client, 
            access_token=auth_token_data.access_token, 
            expires_at=auth_token_data.expires_at_seconds,
            on_refresh=self.token['storage'].store_token_response
        )

    def set_whoami(self):
        """
        Retrieves and prints the active user's identity details using 
        the cached tokens, mimicking the `globus whoami` CLI command.
        """
        
        # Create a new AuthClient specifically bound to the user's authorizer
        bound_auth_client = globus_sdk.AuthClient(authorizer=self.token['auth_authorizer']) if self.token else None
        
        try:
            self.whoami = {}
            user_profile = bound_auth_client.userinfo()
            self.whoami['username'] = user_profile.get('preferred_username') or user_profile.get('username')
            self.whoami['username'] = user_profile.get('name')
            self.whoami['email'] = user_profile.get('email')
            self.whoami['id'] = user_profile.get('sub')
        except Exception as error:
            self.logger.error(f"Failed to fetch user info: {str(error)}")
            self.whoami = None

    def set_endpoint_info(self, endpoint = None):
        """
        Validates if an endpoint/collection is online and responsive.
        Aborts early if the target is down for maintenance or offline.
        """
        if endpoint not in self.endpoints: endpoint = None
        
        if endpoint:
            try:
                endpoint_id = self.source_endpoint if endpoint == "source" else self.destination_endpoint if endpoint == "destination" else None
                if endpoint_id:
                    endpoint_information = self.client.get_endpoint(endpoint_id)
                    endpoint_available = not endpoint_information.get("non_functional")
                    if not endpoint_available:
                        self.logger.error(f"HEALTH CHECK FAILED: ({endpoint_id}) is marked NON-FUNCTIONAL.")
        
                    test_path = endpoint_information.get("default_directory") or "/"
                    for self.endpoint_info in self.client.operation_ls(endpoint_id, path=test_path, limit=1): break
                    
                    self.logger.info(f"HEALTH CHECK PASSED: Verified live connectivity ({endpoint_id}).")
                else: self.endpoint_info = None

            except globus_sdk.TransferAPIError as error:
                if error.code in ["PermissionDenied", "ConsentRequired", "AuthenticationFailed"]:
                    self.logger.error(f"HEALTH CHECK: Verified live connectivity ({endpoint_id}) [Status: {error.code}].")
                else: self.logger.error(f"HEALTH CHECK FAILED: ({endpoint_id}) is unreachable. Code: {error.code} - {error.message}")
                self.endpoint_info = None
            except Exception as error:
                self.logger.error(f"Unexpected error when checking health: {str(error)}")
                self.endpoint_info = None
        else: self.endpoint_info = None
        
    def execute_transfer(self, items=None, options=None):
        """
        Validates both endpoints, dynamically constructs paths, 
        and executes the transfer securely, blocking until completion.
        """
        if items and options:
            label = options['label'] if 'label' in options else "sdss-transfer"
            preserve_mtime = options['preserve_mtime'] if 'preserve_mtime' in options else None
            sync = options['sync'] if 'sync' in options else None
            encrypt = options['encrypt'] if 'encrypt' in options else None
            verify = options['verify'] if 'verify' in options else None
            delete = options['delete'] if 'delete' in options else None
            fail_on_quota_errors = options['fail_on_quota_errors'] if 'fail_on_quota_errors' in options else None
        
            # SDK v4 Requirement: TransferData no longer accepts the transfer_client object
            transfer_data = globus_sdk.TransferData(
                source_endpoint=self.source_endpoint, 
                destination_endpoint=self.destination_endpoint, 
                label=label, 
                preserve_timestamp=preserve_mtime,
                sync_level=sync,
                encrypt_data=encrypt,
                verify_checksum=verify,
                delete_destination_extra=delete,
                fail_on_quota_errors=fail_on_quota_errors
            )
            for label, item in items.items():
                transfer_data.add_item(item['source'], item['destination'], recursive=item['recursive'])
                if self.verbose: print("Add item for label=%(label)r with source=%(source)r and destination=%r(destination)r" % item)
            try:
                self.logger.info(f"Submitting transfer from {source_path} to {destination_directory}...")
                submit_result = self.client.submit_transfer(transfer_data)
                self.task_id = submit_result["task_id"]
                self.logger.info(f"Transfer submitted successfully. Task ID: {task_id}")
            except globus_sdk.TransferAPIError as error:
                self.logger.error(f"Globus Transfer API Error: {error.http_status} - {error.code} - {error.message}")
                self.task_id = None
            except Exception as error:
                self.logger.error(f"Unexpected error during transfer lifecycle: {str(error)}")
                self.task_id = None
        else: self.task_id = None
               
    def wait(self, timeout=86400, polling_interval=10):
        if self.task_id:
            self.logger.info("Waiting for transfer execution...")
            self.client.task_wait(self.task_id, timeout=timeout, polling_interval=polling_interval)
            
            self.task = self.client.get_task(self.task_id)
            self.status = self.task["status"]
            
            if self.status == "SUCCEEDED":
                self.logger.info(f"SUCCESS: Transfer task {task_id} completed smoothly.")
            elif self.status == "FAILED":
                error_message = task.get("fatal_error", "Unknown fatal error occurred.")
                self.logger.error(f"FAILURE: Transfer task {task_id} failed. Reason: {error_message}")
            else:
                self.logger.warning(f"WARNING: Transfer task {task_id} finished with unexpected status: {status}")
        else: self.status = None

                    

