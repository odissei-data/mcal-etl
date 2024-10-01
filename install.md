# MCAL

1. Clone this repository locally, enter the repository directory and switch to the "acceptance" branch:

   ```
   git checkout acceptance
   ```

2. Go to <https://odissei.triply.cc/me/-/settings/tokens>

3. Create a token with write access, and copy the copy.

4. Create a file called `.env` in this repository, and enter the following information:

   ```
   TRIPLYDB_TOKEN={your-token-from-step-3}
   USER=odissei
   ENV="Acceptance"
   ```

5. Run the following commands:

   ```
   yarn
   yarn build
   yarn etl
   ```
