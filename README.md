# Refinery

Refinery contains scripts, artifacts, and configuration for WMF's
analytics cluster.



## Setting up the refinery repository

1. Install git-fat from https://github.com/jedbrown/git-fat on your
   system.

2. Clone the repository.

   You can find the commands to clone the repository at [WMF's gerrit](https://gerrit.wikimedia.org/r/#/admin/projects/analytics/refinery).

   To clone anonymously, just run

   ```
   git clone https://gerrit.wikimedia.org/r/analytics/refinery
   ```

3. change to the cloned repository by running

   ```
   cd refinery
   ```

4. Initialize git-fat by running

   ```
   git fat init
   ```

5. Pull existing artifacts into the repository by running

   ```
   git fat pull
   ```

   (Depending on you internet connection, this step may take some time.)

6. Done.
