#!/usr/bin/env python3
"""GitHub App authentication using only standard library + openssl"""
import json
import time
import base64
import subprocess
import sys

class GitHubApp:
    def __init__(self, app_id, private_key_path):
        self.app_id = app_id
        self.private_key_path = private_key_path
        self.jwt_token = None
        self.installation_token = None

    def create_jwt(self):
        """Create JWT token using openssl for signing"""
        # JWT Header
        header = {"alg": "RS256", "typ": "JWT"}
        header_b64 = base64.urlsafe_b64encode(
            json.dumps(header, separators=(',', ':')).encode()
        ).decode().rstrip('=')

        # JWT Payload
        now = int(time.time())
        payload = {
            "iat": now - 60,  # issued 60 seconds in the past
            "exp": now + 600,  # expires in 10 minutes
            "iss": self.app_id
        }
        payload_b64 = base64.urlsafe_b64encode(
            json.dumps(payload, separators=(',', ':')).encode()
        ).decode().rstrip('=')

        # Message to sign
        message = f"{header_b64}.{payload_b64}"

        # Sign using openssl
        result = subprocess.run(
            ['openssl', 'dgst', '-sha256', '-sign', self.private_key_path],
            input=message.encode(),
            capture_output=True
        )

        if result.returncode != 0:
            raise Exception(f"OpenSSL signing failed: {result.stderr.decode()}")

        # Base64 encode signature
        signature_b64 = base64.urlsafe_b64encode(result.stdout).decode().rstrip('=')

        # Complete JWT
        self.jwt_token = f"{message}.{signature_b64}"
        return self.jwt_token

    def curl(self, method, url, headers=None, data=None):
        """Make HTTP request using curl"""
        cmd = ['curl', '-s', '-X', method]

        # Add headers
        if headers:
            for key, value in headers.items():
                cmd.extend(['-H', f'{key}: {value}'])

        # Add data
        if data:
            cmd.extend(['-d', json.dumps(data)])

        cmd.append(url)

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Curl failed: {result.stderr}")

        try:
            return json.loads(result.stdout)
        except:
            return result.stdout

    def get_installations(self):
        """Get GitHub App installations"""
        if not self.jwt_token:
            self.create_jwt()

        headers = {
            'Authorization': f'Bearer {self.jwt_token}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28'
        }

        return self.curl('GET', 'https://api.github.com/app/installations', headers=headers)

    def get_installation_token(self, installation_id):
        """Get installation access token"""
        if not self.jwt_token:
            self.create_jwt()

        headers = {
            'Authorization': f'Bearer {self.jwt_token}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28'
        }

        response = self.curl(
            'POST',
            f'https://api.github.com/app/installations/{installation_id}/access_tokens',
            headers=headers
        )

        if 'token' in response:
            self.installation_token = response['token']
            return self.installation_token
        else:
            raise Exception(f"Failed to get installation token: {response}")

    def api_call(self, method, endpoint, data=None):
        """Make authenticated API call"""
        if not self.installation_token:
            raise Exception("No installation token. Call get_installation_token first.")

        headers = {
            'Authorization': f'Bearer {self.installation_token}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28'
        }

        url = f'https://api.github.com{endpoint}'
        return self.curl(method, url, headers=headers, data=data)

    def get_workflow_logs(self, owner, repo, job_id):
        """Get logs for a specific job"""
        if not self.installation_token:
            raise Exception("No installation token")

        # Get logs using curl directly (returns text, not JSON)
        cmd = [
            'curl', '-s', '-L',
            '-H', f'Authorization: Bearer {self.installation_token}',
            '-H', 'Accept: application/vnd.github+json',
            '-H', 'X-GitHub-Api-Version: 2022-11-28',
            f'https://api.github.com/repos/{owner}/{repo}/actions/jobs/{job_id}/logs'
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout

# Main execution
if __name__ == "__main__":
    APP_ID = "2184263"
    PRIVATE_KEY_PATH = "/home/postgres/llm-inside-docker.2025-10-26.private-key.pem"

    try:
        app = GitHubApp(APP_ID, PRIVATE_KEY_PATH)

        print("Creating JWT token...")
        app.create_jwt()
        print(f"✓ JWT created (first 50 chars): {app.jwt_token[:50]}...")

        print("\nGetting installations...")
        installations = app.get_installations()
        print(f"✓ Found {len(installations)} installation(s)")

        if installations:
            installation_id = installations[0]['id']
            print(f"\nUsing installation ID: {installation_id}")

            print("Getting installation token...")
            app.get_installation_token(installation_id)
            print(f"✓ Installation token obtained (first 20 chars): {app.installation_token[:20]}...")

            # Test: Get repository info
            print("\nTesting API access - getting repo info...")
            repo = app.api_call('GET', '/repos/asah/smol')
            print(f"✓ Repository: {repo.get('full_name', 'unknown')}")

            # Get the latest CI workflow run
            runs = app.api_call('GET', '/repos/asah/smol/actions/runs?per_page=10')
            ci_runs = [r for r in runs.get('workflow_runs', []) if r['name'] == 'CI']
            latest_run = ci_runs[0] if ci_runs else None

            if latest_run:
                run_id = latest_run['id']
                print(f"\nGetting workflow run {run_id}...")
                print(f"Status: {latest_run['status']} | Conclusion: {latest_run.get('conclusion')} | Commit: {latest_run['head_sha'][:7]}")
                jobs = app.api_call('GET', f'/repos/asah/smol/actions/runs/{run_id}/jobs')
            else:
                print("No workflow runs found!")
                jobs = {'jobs': []}

            # Find the test job
            for job in jobs.get('jobs', []):
                if 'test' in job['name'].lower() or 'coverage' in job['name'].lower():
                    print(f"\nFound job: {job['name']} (ID: {job['id']})")
                    print(f"Job conclusion: {job['conclusion']}")
                    print("Fetching logs...")

                    logs = app.get_workflow_logs('asah', 'smol', job['id'])

                    # Find relevant sections
                    lines = logs.split('\n')

                    # If failed, show coverage errors
                    if job['conclusion'] == 'failure':
                        in_coverage = False
                        error_lines = []

                        for i, line in enumerate(lines):
                            if 'Build and test (coverage)' in line or 'COVERAGE=1 make' in line:
                                in_coverage = True

                            if in_coverage:
                                error_lines.append(line)
                                # Stop after we see the error
                                if 'Error: Process completed with exit code' in line:
                                    break

                        print("\n=== COVERAGE BUILD LOGS (ERRORS) ===")
                        # Print last 100 lines of coverage section
                        print('\n'.join(error_lines[-100:]))
                    else:
                        # If success, show cache and coverage check
                        cache_lines = []
                        coverage_lines = []

                        for i, line in enumerate(lines):
                            if 'Install and cache PostgreSQL' in line or 'cache-apt-pkgs-action' in line:
                                # Capture next 30 lines
                                cache_lines = lines[i:i+30]

                            if 'Check coverage' in line or 'calc_cov.sh' in line:
                                # Capture next 20 lines
                                coverage_lines = lines[i:i+20]

                        if cache_lines:
                            print("\n=== CACHE STEP ===")
                            print('\n'.join(cache_lines))

                        if coverage_lines:
                            print("\n=== COVERAGE CHECK ===")
                            print('\n'.join(coverage_lines))
                    break
        else:
            print("No installations found!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
