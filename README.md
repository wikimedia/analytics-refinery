# Refinery

Refinery contains scripts, artifacts, and configuration for WMF's
analytics cluster.


## Setting up the refinery repository

1. Install git-lfs on your system.  Debian includes a git-lfs package.

2. Make sure the ```docopt``` and ```dateutil``` Python packages are
   available on your system.

   On Ubuntu systems, you can achiev this by running

   ```
   sudo apt-get install python-docopt
   sudo apt-get install python-dateutil
   ```

3. Clone the repository.

   You can find the commands to clone the repository at [WMF's gerrit](https://gerrit.wikimedia.org/r/#/admin/projects/analytics/refinery).

   To clone anonymously, just run

   ```
   git clone https://gerrit.wikimedia.org/r/analytics/refinery
   ```

4. change to the cloned repository by running

   ```
   cd refinery
   ```

5. Initialize git-lfs by running

   ```
   git lfs install
   ```

6. Pull existing artifacts into the repository by running

   ```
   git lfs pull
   ```

   (Depending on you internet connection, this step may take some time.)

7. Add the ```refinery/python``` directory to your ```PYTHONPATH```.

   To add it only in the running shell, you can use

   ```
   export PYTHONPATH=/path/to/analytics/refinery/python
   ```

   Please refer to your operating system's documentation on how to do
   this globally.

8. Done.


## Oozie job naming convention

* Job base names is following directory pattern in the oozie directory,
    replacing slashes with dashes. For instance `webrequest/load/bundle.xml` job
    is named `webrequest-load-bundle`, and `last_access_uniques/daily/coordinator.xml`
    is named `last_access_uniques-daily-coord`.
* Root job names end either in `-bundle` or `-coord`, while children job names
   end with job parameters separated with dashes.
