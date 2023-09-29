/*
 * Copyright 2007-2014 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    edu.isi.pegasus.common.util.CommonPropertiesTest.class,
    edu.isi.pegasus.common.util.CondorVersionTest.class,
    edu.isi.pegasus.common.util.VersionTest.class,
    edu.isi.pegasus.common.util.PegasusURLTest.class,
    edu.isi.pegasus.common.util.FileDetectorTest.class,
    edu.isi.pegasus.common.util.GliteEscapeTest.class,
    edu.isi.pegasus.common.util.VariableExpanderTest.class,
    edu.isi.pegasus.planner.namespace.PegasusTest.class,
    edu.isi.pegasus.planner.namespace.MetadataTest.class,
    edu.isi.pegasus.planner.catalog.replica.ReplicaFactoryTest.class,
    edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStoreTest.class,
    edu.isi.pegasus.planner.catalog.replica.impl.RegexRCTest.class,
    edu.isi.pegasus.planner.catalog.replica.impl.YAMLRCTest.class,
    edu.isi.pegasus.planner.catalog.replica.impl.MetaRCTest.class,
    edu.isi.pegasus.planner.catalog.replica.impl.JDBCRCTest.class,
    edu.isi.pegasus.planner.catalog.replica.impl.SimpleFileTest.class,
    edu.isi.pegasus.planner.catalog.site.SiteFactoryTest.class,
    edu.isi.pegasus.planner.catalog.site.impl.XMLTest.class,
    edu.isi.pegasus.planner.catalog.site.impl.YAMLTest.class,
    edu.isi.pegasus.planner.catalog.site.classes.DirectoryTest.class,
    edu.isi.pegasus.planner.catalog.site.classes.FileServerTest.class,
    edu.isi.pegasus.planner.catalog.site.classes.GridGatewayTest.class,
    edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntryTest.class,
    edu.isi.pegasus.planner.catalog.transformation.TransformationFactoryTest.class,
    edu.isi.pegasus.planner.catalog.transformation.classes.TransformationCatalogEntryTest.class,
    edu.isi.pegasus.planner.catalog.transformation.classes.ContainerTest.class,
    edu.isi.pegasus.planner.catalog.transformation.classes.TransformationTest.class,
    edu.isi.pegasus.planner.catalog.transformation.mapper.AllTest.class,
    edu.isi.pegasus.planner.catalog.transformation.mapper.StagedTest.class,
    edu.isi.pegasus.planner.catalog.transformation.impl.TextTest.class,
    edu.isi.pegasus.planner.catalog.transformation.impl.YAMLTest.class,
    edu.isi.pegasus.planner.classes.PegasusFileTest.class,
    edu.isi.pegasus.planner.classes.JobTest.class,
    edu.isi.pegasus.planner.classes.NotificationsTest.class,
    edu.isi.pegasus.planner.cluster.RuntimeClusteringTest.class,
    edu.isi.pegasus.planner.code.GridStartTest.class,
    edu.isi.pegasus.planner.code.generator.condor.CondorEnvironmentEscapeTest.class,
    edu.isi.pegasus.planner.code.generator.condor.PegasusSubmitDAGTest.class,
    edu.isi.pegasus.planner.code.generator.condor.style.GLiteTest.class,
    edu.isi.pegasus.planner.code.generator.condor.style.CondorTest.class,
    edu.isi.pegasus.planner.code.generator.condor.style.CondorGTest.class,
    edu.isi.pegasus.planner.common.PegasusConfigurationTest.class,
    edu.isi.pegasus.planner.mapper.output.FlatOutputMapperTest.class,
    edu.isi.pegasus.planner.mapper.output.HashedOutputMapperTest.class,
    edu.isi.pegasus.planner.mapper.output.ReplicaOutputMapperTest.class,
    edu.isi.pegasus.planner.mapper.output.FixedOutputMapperTest.class,
    edu.isi.pegasus.planner.refiner.DataReuseEngineTest.class,
    edu.isi.pegasus.planner.refiner.InterPoolEngineTest.class,
    edu.isi.pegasus.planner.partitioner.graph.CycleCheckerTest.class,
    edu.isi.pegasus.planner.parser.DAXParserFactoryTest.class,
    edu.isi.pegasus.planner.parser.dax.DAXParser3Test.class,
    edu.isi.pegasus.planner.parser.dax.DAXParser5Test.class,
    edu.isi.pegasus.planner.dax.ADAGTest.class,
    edu.isi.pegasus.planner.transfer.generator.StageOutTest.class,
    edu.isi.pegasus.planner.transfer.generator.StageInTest.class,
    edu.isi.pegasus.planner.transfer.sls.TransferTest.class,
    edu.isi.pegasus.planner.transfer.sls.CondorTest.class
})
public class AllTests {}
