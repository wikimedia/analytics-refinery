# Coding conventions

* For files underneath ```bin```:
   * Use ```-``` as separator for filenames (like
     ```my-nice-cool-script```).

   * Do not use extensions for the used language (so no trailing
     ```.sh```, and no trailing ```.py```)

   * At least for programs that “act on” refinery, filenames should
     start in ```refinery-```.

* For files outside of ```bin```:
  * Use ```_``` as separator for filenames (like
    ```my_nice_file.txt```).

  * Do use proper extensions there. (like trailing ```.xml```, or
    trailing ```.properties```)

* We do not like camelCase in variable names. Only use them in Java
  code, where they are somewhat standard, and in Python class names.

* Filenames for files in HiveQL should end in ```.hql```.

* When referencing tables, reference them using ```database.table```
  wherever possible.

* For Hadoop jobs, use the “adhoc” queue per default, and make sure
  that it can be overridden to submit the job to queues with less
  limitations.

* For Oozie jobs, set ```oozie.use.system.libpath```, and
  ```oozie.action.external.stats.write``` to ```true``` in the
  corresponding properties files. (This setting will get added as
  default at some point, but until then, we rely on it being set in
  the properties files)

* When needing to reference hdfs, try using ```hdfs:///```. Where this
  does not work, try using ```hdfs://analytics-hadoop/```, then
  ```hdfs://namenode.analytics.eqiad.wmnet/``` and finally
  ```hdfs://namenode.analytics.eqiad.wmnet:8020/```.

* Use 4 spaces to indent files.

* There is no rule on vertical alignment.

* In HiveQL, we prefer ```!=``` over ```<>```.

* In XML, put the beginning and ending tag on separate lines for
  multi-lined element values. So use


      <tag>
        foo
        bar
      </tag>

  instead of

      <tag>foo
      bar</tag>

  .

* For Oozie jobs, have the name end in ```-wf``` for workflows,
  ```-coord``` for coordinators, and ```-bundle``` for bundles.

* If you add Oozie jobs, update the ```diagrams/oozie-overview.dia```.