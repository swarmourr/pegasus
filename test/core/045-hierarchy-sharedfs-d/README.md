A hierarchal workflow with two sub workflows with each sub workflow listing the files it requires and generates.
The sub workflow run on the default condorpool site
This is something that is being supported since 5.0 release
PM-1766 this test is to ensure that data reuse is not triggered when planning the first sub workflow
Now the cache file that is passed to the sub workflow job is job specific and not the cache file of the whole root workflow and does not trigger faulty data reuse
Also tests PM-1765 as we don't specify any output sites for sub workflows. and the planner automatically generates the --output-map option for sub workflows
