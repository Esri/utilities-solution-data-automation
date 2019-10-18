import datetime
import os
import arcpy
from arcpy import env
from copy import deepcopy
import gc
import time
# New CSV exporter class
import csvexport as CSVExport
import sys
from dateutil.parser import parse
from . import dataprep as DataPrep
from . import common as Common
from . import gptools
from distutils.util import strtobool

import subprocess

dateTimeFormat = '%Y-%m-%d %H:%M'
tempCSVName = "mergedreport"


# ----------------------------------------------------------------------
def trace():
    """
        trace finds the line, the filename
        and error message and returns it
        to the user
    """
    import traceback, inspect
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    filename = inspect.getfile(inspect.currentframe())
    # script name + line number
    line = tbinfo.split(", ")[1]
    # Get Python syntax error
    #
    synerror = traceback.format_exc().splitlines()[-1]
    return line, filename, synerror


class ReportToolsError(Exception):
    """ raised when error occurs in utility module functions """
    pass


def getLayerName(fc):
    fcName = os.path.basename(fc)
    if '.' in fcName:
        fcSplit = fcName.split('.')
        fcName = fcSplit[len(fcSplit) - 1]
    return fcName


# ----------------------------------------------------------------------
def reportDataPrep(reportConfig):
    try:
        print " "
        print "-----Data Prep Section Starting-----"

        startTime = datetime.datetime.now()

        if 'ExportDataLocally' in reportConfig and (reportConfig['ExportDataLocally'].upper() == "YES" or reportConfig[
            'ExportDataLocally'].upper() == "TRUE"):
            if ('TempExportLocation' in reportConfig and reportConfig["TempExportLocation"] != ""):
                startSectTime = datetime.datetime.now()
                print "Data Copy starting: %s" % (startSectTime.strftime(dateTimeFormat))

                outputWorkspace = reportConfig["TempExportLocation"]
                _CheckCreateGDBProcess(outputWorkspace)
                if 'ReportingAreas' in reportConfig:
                    if arcpy.Exists(dataset=reportConfig['ReportingAreas']) == True:
                        fcName = getLayerName(reportConfig['ReportingAreas'])
                        reportConfig['ReportingAreas'] = str(
                            arcpy.FeatureClassToFeatureClass_conversion(reportConfig['ReportingAreas'], outputWorkspace,
                                                                        fcName))
                        # arcpy.ClearWorkspaceCache_management(reportConfig['ReportingAreas'])
                        print "Completed copy on {0}".format(fcName)
                    else:
                        raise ReportToolsError({
                            "function": "reportDataPrep",
                            "line": 62,
                            "filename": "reportTools.py",
                            "synerror": reportConfig['ReportingAreas'] + " does not exist",
                        })


                else:
                    print "Warning: ReportingAreas parameter is missing"
                if 'Data' in reportConfig and isinstance(reportConfig['Data'], dict):
                    for key, featClass in reportConfig['Data'].items():
                        if arcpy.Exists(dataset=featClass) == True:
                            fcName = getLayerName(fc=featClass)
                            fcRes = arcpy.FeatureClassToFeatureClass_conversion(featClass, outputWorkspace, fcName)
                            # arcpy.ClearWorkspaceCache_management(featClass)
                            reportConfig['Data'][key] = str(fcRes)

                            print "Completed copy on {0}".format(fcName)
                        else:
                            reportConfig['Data'][key] = featClass
                else:
                    print "Warning: Data section is missing"
                print "Data Copy complete, time to complete: %s" % str(datetime.datetime.now() - startSectTime)
        if 'PreProcessingTasks' in reportConfig and reportConfig['PreProcessingTasks']:

            startSectTime = datetime.datetime.now()
            print " "
            print "Preprocessing: %s" % (startSectTime.strftime(dateTimeFormat))

            for process in reportConfig['PreProcessingTasks']:
                if process["ToolType"].upper() == "MODEL":
                    if arcpy.Exists(process["ToolPath"]):
                        arcpy.ImportToolbox(process["ToolPath"])
                        arcpy.gp.AddToolbox(process["ToolPath"])
                        for tool in process["Tools"]:
                            if hasattr(arcpy, tool):
                                getattr(arcpy, tool)()
                                print "Finished executing model {0}".format(tool)
                            elif hasattr(arcpy.gp, "{}_{}".format(tool, arcpy.gp.toolbox)):
                                getattr(arcpy.gp, "{}_{}".format(tool, arcpy.gp.toolbox))()
                                print "Finished executing model {0}".format(tool)
                            else:
                                print "%s was not found, please verify the name" % tool
                    else:
                        print "%s was not found, please verify the path" % process["ToolPath"]
                elif process["ToolType"].upper() == "SCRIPT":
                    for tool in process["Tools"]:
                        scriptPath = process["ToolPath"] + "/" + tool
                        subprocess.call([sys.executable, os.path.join(scriptPath)])
                        print "Finished executing script {0}".format(tool)
                else:
                    print "Sorry, not a valid tool"
            print "PreProcess complete, time to complete: %s" % str(datetime.datetime.now() - startSectTime)
        arcpy.ClearWorkspaceCache_management()
        print "-----Data Prep Section complete, time to complete: %s-----" % str(datetime.datetime.now() - startTime)
        return reportConfig
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "CopyData",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        })
    except (ReportToolsError), e:
        raise e
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "CopyData",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })


# ----------------------------------------------------------------------
def _CheckCreateGDBProcess(outputWorkspace):
    try:
        # If user param is to overwrite GDB, then delete it first
        if arcpy.Exists(outputWorkspace) == True:
            arcpy.Delete_management(outputWorkspace)
            print "Deleted previous GDB {0}".format(outputWorkspace)

        # if the local gdb doesn't exist, then create it using the path and name given in the end_db string
        if arcpy.Exists(outputWorkspace) == False:
            if outputWorkspace.rfind("\\") != -1:
                lastSlash = outputWorkspace.rfind("\\")
            else:
                lastSlash = outputWorkspace.rfind("/")
            arcpy.CreateFileGDB_management(outputWorkspace[:lastSlash], outputWorkspace[lastSlash + 1:])

            print "Created geodatabase {0}".format(outputWorkspace[lastSlash + 1:])


    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "_CheckCreateGDBProcess",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        })
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "_CheckCreateGDBProcess",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })


# ----------------------------------------------------------------------
def create_report_layers_using_config(config):
    arcpy.env.overwriteOutput = True

    reporting_areas_ID_field = None
    reporting_areas = None
    reports = None
    report_msg = None
    idRes = None
    try:

        if arcpy.Exists(config["ResultsGDB"]) == False:

            gdbName = os.path.basename(config["ResultsGDB"])
            if not '.sde' in gdbName:
                path = os.path.dirname(config["ResultsGDB"])
                if path == '':
                    path = os.getcwd()
                    config["ResultsGDB"] = os.path.join(path, gdbName)
                arcpy.CreateFileGDB_management(out_folder_path=path,
                                               out_name=gdbName,
                                               out_version=None)
                print "%s created in %s" % (gdbName, path)

        # To handle CSV export, a temp FC is created.  This code just checks and deletes it, if it exist.

        _TempFC = os.path.join(config["ResultsGDB"], tempCSVName)
        deleteFC([_TempFC])
        reports = config['Reports']

        report_msg = []

        if 'ReportingAreas' in config:
            reporting_areas = config['ReportingAreas']

            reporting_areas_ID_field = config['ReportingAreasIDField']

            if arcpy.Exists(reporting_areas) == False:
                raise ReportToolsError({
                    "function": "create_report_layers_using_config",
                    "line": 61,
                    "filename": 'reporttools',
                    "synerror": 'Report data cannot be located'
                })
            idRes = validate_id_field(reporting_areas=reporting_areas, report_ID_field=reporting_areas_ID_field)
            if not idRes is None:
                reporting_areas = idRes['ReportLayer']
                reporting_areas_ID_field = idRes['IDField']
        if arcpy.Exists(config['SchemaGDB']) == False:
            raise ReportToolsError({
                "function": "create_report_layers_using_config",
                "line": 61,
                "filename": 'reporttools',
                "synerror": '%s is not valid in the SchemaGDB parameter' % config['SchemaGDB']
            }
            )

            # if not os.path.isabs(reporting_areas):
            # reporting_areas =os.path.abspath(reporting_areas)
        for i in reports:
            if not ('RunReport' in i):
                i['RunReport'] = 'yes'
                print "Report is missing the RunReport parameter:  type string, values, True or False"
            if 'RunReport' in i and (i['RunReport'].upper() == "YES" or i['RunReport'].upper() == "TRUE"):
                datasetName = os.path.dirname(i['ReportResult'])
                layerName = os.path.basename(i['ReportResult'])
                datasetPath = os.path.join(config["ResultsGDB"], datasetName)
                reportSchema = os.path.join(config['SchemaGDB'], i['ReportResultSchema'])
                if arcpy.Exists(reportSchema) == False:
                    raise ReportToolsError({
                        "function": "create_report_layers_using_config",
                        "line": 61,
                        "filename": 'reporttools',
                        "synerror": 'Report Schema %s is not valid' % reportSchema
                    }
                    )
                if arcpy.Exists(datasetPath) == False and (datasetName != layerName):

                    datasetNameSch = os.path.dirname(i['ReportResultSchema'])
                    layerNameSch = os.path.basename(i['ReportResultSchema'])
                    if layerNameSch != datasetNameSch:
                        reportSchema = os.path.join(config['SchemaGDB'], datasetNameSch)
                    else:
                        reportSchema = os.path.join(config['SchemaGDB'], layerNameSch)
                    reportSpatRef = arcpy.Describe(reportSchema).spatialReference
                    arcpy.CreateFeatureDataset_management(out_dataset_path=config["ResultsGDB"], out_name=datasetName,
                                                          spatial_reference=reportSpatRef)
                    print "%s feature dataset created in %s" % (datasetName, config["ResultsGDB"])
                del datasetName
                del datasetPath
                del layerName
                if i['Type'].upper() == "JOINCALCANDLOAD":
                    create_calcload_report(report_params=i, datasources=config)
                elif i['Type'].upper() == "RECLASS":
                    if arcpy.Exists(reporting_areas) == False:
                        raise ReportToolsError({
                            "function": "create_report_layers_using_config",
                            "line": 61,
                            "filename": 'reporttools',
                            "synerror": 'Report data cannot be located'
                        })
                    report_msg.append(create_reclass_report(reporting_areas=reporting_areas,
                                                            reporting_areas_ID_field=reporting_areas_ID_field,
                                                            report_params=i, datasources=config))
                elif i['Type'].upper() in ["AVERAGE", "STATISTIC"]:
                    if arcpy.Exists(reporting_areas) == False:
                        raise ReportToolsError({
                            "function": "create_report_layers_using_config",
                            "line": 61,
                            "filename": 'reporttools',
                            "synerror": 'Report data cannot be located'
                        })
                    report_msg.append(create_average_report(reporting_areas=reporting_areas,
                                                            reporting_areas_ID_field=reporting_areas_ID_field,
                                                            report_params=i, datasources=config))
                else:
                    print "Unsupported report type"

        # After all the different reports have been run, export a single CSV of results.
        csvProcess = CSVExport.CSVExport(CSVLocation=config["CSVOutputLocation"],
                                         layer=_TempFC,
                                         workspace=config["ResultsGDB"])
        report_msg.append(csvProcess.WriteCSV())

        if not idRes is None:
            deleteFC(in_datasets=[idRes['ReportLayer']])
        if 'error' in report_msg:
            return False
        else:
            return True


    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_report_layers_using_config",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        })
    except (ReportToolsError), e:
        raise e
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_report_layers_using_config",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )

    finally:
        reports = None
        report_msg = None
        reporting_areas_ID_field = None
        reporting_areas = None
        del reports
        del report_msg
        del reporting_areas_ID_field
        del reporting_areas

        gc.collect()


# ----------------------------------------------------------------------
def create_calcload_report(report_params, datasources):
    filt_layer = None
    reporting_layer = None
    reporting_layer_id_field = None
    joinInfo = None
    field_map = None
    sql = None
    report_date = None
    report_schema = None
    report_result = None
    _tempWorkspace = None
    _tempTableName = None
    _tempTableFull = None
    _procData = None
    inputCnt = None
    try:

        filt_layer = "filter_layer"

        reporting_layer = datasources["Data"][report_params['Data']]
        reporting_layer_id_field = report_params['DataIDField']
        joinInfo = report_params['JoinInfo']
        reporting_areas = joinInfo['FeatureData']
        reporting_areas_ID_field = joinInfo['FeatureDataIDField']
        field_map = report_params['FieldMap']

        sql = report_params['FilterSQL']

        report_date = report_params["ReportDateField"]
        report_schema = datasources["SchemaGDB"] + "/" + report_params['ReportResultSchema']
        report_result = datasources["ResultsGDB"] + "/" + report_params['ReportResult']

        # if not os.path.isabs(report_result):
        # report_result = os.path.abspath( report_result)

        # if not os.path.isabs(report_schema):
        # report_schema = os.path.abspath( report_schema)

        _tempWorkspace = env.scratchGDB
        _tempTableName = Common.random_string_generator()
        _tempTableFull = os.path.join(_tempWorkspace, _tempTableName)

        if sql == '' or sql is None or sql == '1=1' or sql == '1==1':
            filt_layer = reporting_layer
        else:
            # arcpy.MakeQueryTable_management(in_table=reporting_layer,out_table=filt_layer,in_key_field_option="USE_KEY_FIELDS",in_key_field="#",in_field="#",where_clause=sql)
            try:
                arcpy.MakeFeatureLayer_management(reporting_layer, filt_layer, sql, "", "")

            except:
                try:
                    arcpy.TableToTable_conversion(in_rows=reporting_layer, out_path=_tempWorkspace,
                                                  out_name=_tempTableName)

                    arcpy.MakeTableView_management(in_table=_tempTableFull, out_view=filt_layer, where_clause=sql,
                                                   workspace="#", field_info="#")

                except:
                    pass
        inputCnt = int(arcpy.GetCount_management(in_rows=filt_layer)[0])
        className = os.path.basename(report_result)
        layerPath = os.path.dirname(report_result)
        arcpy.FeatureClassToFeatureClass_conversion(in_features=report_schema,
                                                    out_path=layerPath,
                                                    out_name=className,
                                                    where_clause=None,
                                                    field_mapping=None,
                                                    config_keyword=None)
        # arcpy.Copy_management(report_schema,report_result,"FeatureClass")

        if inputCnt == 0:

            print "%s was created or updated" % report_result
        else:

            _procData = calculate_load_results(feature_data=reporting_areas,
                                               feature_data_id_field=reporting_areas_ID_field,
                                               join_table=filt_layer,
                                               join_table_id_field=reporting_layer_id_field,
                                               report_date_field=report_date,
                                               report_result=report_result,
                                               field_map=field_map
                                               )

            deleteFC([_procData])
            if arcpy.Exists(_tempTableFull):
                deleteFC([_tempTableFull])

            print "%s was created or updated" % report_result

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_calcload_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_calcload_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        filt_layer = None
        reporting_layer = None
        reporting_layer_id_field = None
        joinInfo = None
        field_map = None
        sql = None
        report_date = None
        report_schema = None
        report_result = None
        _tempWorkspace = None
        _tempTableName = None
        _tempTableFull = None
        _procData = None
        inputCnt = None

        del filt_layer
        del reporting_layer
        del reporting_layer_id_field
        del joinInfo
        del field_map
        del sql
        del report_date
        del report_schema
        del report_result
        del _tempWorkspace
        del _tempTableName
        del _tempTableFull
        del _procData
        del inputCnt

        gc.collect()


# ----------------------------------------------------------------------
def create_reclass_report(reporting_areas, reporting_areas_ID_field, report_params, datasources):
    classified_layer_field_name = None
    filt_layer = None
    reporting_layer = None
    field_map = None
    sql = None
    count_field = None
    reclass_map = None
    report_schema = None
    report_result = None
    report_date_field = None
    report_ID_field = None
    result_exp = None
    inputCnt = None
    classified_layer = None
    pivot_layer = None
    report_copy = None
    try:

        classified_layer_field_name = "reclass"

        filt_layer = "filter_layer"

        # reporting_layer = report_params['Data']
        if not report_params['Data'] in datasources["Data"]:
            print "ERROR: missing %s in the data section of the config" % report_params['Data']
            raise ReportToolsError({
                "function": "create_reclass_report",
                "line": 285,
                "filename": "reportTools",
                "synerror": 'A report has an invalid data section'
            }
            )
        reporting_layer = datasources["Data"][report_params['Data']]

        matching_fields = arcpy.ListFields(dataset=reporting_layer,
                                           wild_card=reporting_areas_ID_field,
                                           field_type=None)

        matched_field = filter(lambda field: field.name == reporting_areas_ID_field, matching_fields)
        if len(matched_field) > 0:
            raise ReportToolsError({
                "function": "create_reclass_report",
                "line": 285,
                "filename": "reportTools",
                "synerror": 'The data cannot contain the reporting areas id field.'
            }
            )
        sql = report_params['FilterSQL']

        count_field = report_params['CountField']
        reclass_map = report_params['ReclassMap']
        adjust_count = report_params.get("AdjustCountField", False)
        if isinstance(adjust_count, str):
            if adjust_count.upper() == 'TRUE':
                adjust_count = True
            else:
                adjust_count = False
        if 'ReclassType' in report_params:
            reclass_type = report_params['ReclassType']
        else:
            reclass_type = 'split'
        report_schema = os.path.join(datasources["SchemaGDB"], report_params['ReportResultSchema'])
        if arcpy.Exists(report_schema) == False:
            raise ReportToolsError({
                "function": "create_reclass_report",
                "line": 285,
                "filename": "reportTools",
                "synerror": '%s does not exist' % report_schema
            }
            )
        report_result = os.path.join(datasources["ResultsGDB"], report_params['ReportResult'])

        if 'FieldMap' in report_params:
            field_map = report_params['FieldMap']
        else:
            field_map = []

            # if not os.path.isabs(report_result):
            # report_result =os.path.abspath( report_result)

            # if not os.path.isabs(report_schema):
            # report_schema =os.path.abspath( report_schema)
        if 'ReportOutputType' in report_params:
            report_output_type = report_params['ReportOutputType']

        else:
            print "Report is missing the ReportOutputType parameter:  type string, values: Overwrite, Append, Update"
            report_output_type = 'Overwrite'
        if 'ReportAreasOverlap' in report_params:
            report_areas_overlap = bool(strtobool(report_params['ReportAreasOverlap']))
        else:
            print "Report is missing the ReportAreasOverlap parameter:  type string, values: True, False"
            report_areas_overlap = True

        if not report_areas_overlap:
            intersect_name = os.path.join("in_memory", Common.random_string_generator())

            arcpy.Intersect_analysis(in_features="'{}' #".format(reporting_areas),
                                     out_feature_class=intersect_name,
                                     join_attributes='ONLY_FID',
                                     cluster_tolerance=None,
                                     output_type='INPUT')
            overlap_count = int(arcpy.GetCount_management(intersect_name)[0])
            if overlap_count:
                print "Warning: The reporting areas have overlapping features and specified ReportAreasOverlap is false in your configuration."

        reporting_areas_filter = None
        if 'ReportingAreasFilter' in report_params and report_params['ReportingAreasFilter'] != "":
            filt_report_layer = 'filteredReportingAreas'
            reporting_areas_filter = report_params['ReportingAreasFilter']
            arcpy.MakeFeatureLayer_management(reporting_areas, filt_report_layer, reporting_areas_filter, "", "")
        else:
            filt_report_layer = reporting_areas
            reporting_areas_filter = ''
        report_date_field = report_params['ReportDateField']
        report_ID_field = report_params['ReportIDField']
        result_exp = report_params['FinalResultExpression']

        if 'UseArcMapExpression' in report_params:
            useArcMapExpression = report_params['UseArcMapExpression']
        else:
            useArcMapExpression = False

        # if type(value_field) is tuple:
        #    average_value = value_field[1]
        #    value_field = value_field[0]

        validate_schema_map(report_schema=report_schema,
                            reclass_map=reclass_map,
                            report_date_field=report_date_field,
                            report_ID_field=report_ID_field)

        if sql == '' or sql is None or sql == '1=1' or sql == '1==1':
            filt_layer = reporting_layer
        else:
            # arcpy.MakeQueryTable_management(in_table=reporting_layer,out_table=filt_layer,in_key_field_option="USE_KEY_FIELDS",in_key_field="#",in_field="#",where_clause=sql)
            try:
                arcpy.MakeFeatureLayer_management(reporting_layer, filt_layer, sql, "", "")
            except arcpy.ExecuteError:
                line, filename, synerror = trace()
                if 'Description' in report_params:
                    errorString = "Report Description: %s" % report_params['Description']
                else:
                    errorString = synerror
                raise ReportToolsError({
                    "function": "create_reclass_report: %s" % reporting_layer,
                    "line": line,
                    "filename": filename,
                    "synerror": errorString,
                    "arcpyError": arcpy.GetMessages(2),
                })

        inputCnt = int(arcpy.GetCount_management(in_rows=filt_layer)[0])
        if inputCnt == 0:
            copy_empty_report(reporting_areas=filt_report_layer, reporting_areas_ID_field=reporting_areas_ID_field,
                              report_schema=report_schema,
                              report_result=report_result,
                              reclass_map=reclass_map,
                              report_date_field=report_date_field,
                              report_ID_field=report_ID_field,
                              reportParam=report_params,
                              config=datasources, report_output_type=report_output_type)
            print "%s was created or updated" % report_result
        else:
            # print "at split_reclass"
            classified_layer = split_reclass(reporting_areas=filt_report_layer,
                                             reporting_areas_ID_field=reporting_areas_ID_field,
                                             reporting_layer=filt_layer,
                                             field_map=field_map,
                                             reclass_map=reclass_map,
                                             classified_layer_field_name=classified_layer_field_name,
                                             count_field_name=count_field,
                                             use_arcmap_expression=useArcMapExpression,
                                             reclass_type=reclass_type,
                                             adjust_count=adjust_count,
                                             report_areas_overlap=report_areas_overlap)

            # print "at classified_pivot"
            pivot_layer = classified_pivot(classified_layer=classified_layer,
                                           classified_layer_field_name=classified_layer_field_name,
                                           reporting_areas_ID_field=reporting_areas_ID_field, count_field=count_field)
            # print "at copy_report_data_schema"
            report_copy = copy_report_data_schema(reporting_areas=filt_report_layer,
                                                  reporting_areas_ID_field=reporting_areas_ID_field,
                                                  report_schema=report_schema,
                                                  report_result=report_result, join_layer=pivot_layer,
                                                  report_output_type=report_output_type)

            # print "at calculate_report_results"
            calculate_report_results(report_result=report_result, reporting_areas_ID_field=reporting_areas_ID_field,
                                     report_copy=report_copy,
                                     reclass_map=reclass_map, report_date_field=report_date_field,
                                     report_ID_field=report_ID_field,
                                     exp=result_exp, reportParam=report_params, config=datasources,
                                     use_arcmap_expression=useArcMapExpression,
                                     report_output_type=report_output_type, delete_append_sql=reporting_areas_filter)
            # print "at deleteFC"
            deleteFC([classified_layer, pivot_layer, report_copy])

            print "%s was created or updated" % report_result

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_reclass_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        })
    except (ReportToolsError), e:
        raise e
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_reclass_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        classified_layer_field_name = None
        filt_layer = None
        reporting_layer = None
        field_map = None
        sql = None
        count_field = None
        reclass_map = None
        report_schema = None
        report_result = None
        report_date_field = None
        report_ID_field = None
        result_exp = None
        inputCnt = None
        classified_layer = None
        pivot_layer = None
        report_copy = None

        del classified_layer_field_name
        del filt_layer
        del reporting_layer
        del field_map
        del sql
        del count_field
        del reclass_map
        del report_schema
        del report_result
        del report_date_field
        del report_ID_field
        del result_exp
        del inputCnt
        del classified_layer
        del pivot_layer
        del report_copy
        gc.collect()


# ----------------------------------------------------------------------
def create_average_report(reporting_areas, reporting_areas_ID_field, report_params, datasources):
    filt_layer = None
    reporting_layer = None
    field_map = None
    sql = None
    code_exp = None
    report_schema = None
    report_result = None
    report_date_field = None
    report_ID_field = None
    avg_field_map = None
    inputCnt = None
    result = None
    report_copy = None

    try:

        filt_layer = "filter_layer"
        reporting_layer = datasources["Data"][report_params['Data']]
        if 'FieldMap' in report_params:
            field_map = report_params['FieldMap']
        else:
            field_map = []
        sql = report_params['FilterSQL']

        code_exp = report_params['PreCalcExpression']

        report_schema = os.path.join(datasources["SchemaGDB"], report_params['ReportResultSchema'])
        report_result = os.path.join(datasources["ResultsGDB"], report_params['ReportResult'])

        # if not os.path.isabs(report_result):
        # report_result =os.path.abspath( report_result)

        # if not os.path.isabs(report_schema):
        # report_schema =os.path.abspath( report_schema)

        if 'ReportOutputType' in report_params:
            report_output_type = report_params['ReportOutputType']

        else:
            print "Report is missing the ReportOutputType parameter:  type string, values: Overwrite, Append, Update"
            report_output_type = 'Overwrite'

        if 'ReportAreasOverlap' in report_params:
            report_areas_overlap = bool(strtobool(report_params['ReportAreasOverlap']))

        else:
            print "Report is missing the ReportAreasOverlap parameter:  type string, values: True, False"
            report_areas_overlap = True

        report_date_field = report_params['ReportDateField']
        report_ID_field = report_params['ReportIDField']

        avg_field_map = report_params['AverageToResultFieldMap']

        if 'UseArcMapExpression' in report_params:
            useArcMapExpression = report_params['UseArcMapExpression']
        else:
            useArcMapExpression = False
        adjust_count = report_params.get("AdjustCountField", False)

        if sql == '' or sql is None or sql == '1=1' or sql == '1==1':
            filt_layer = reporting_layer
        else:
            # arcpy.MakeQueryTable_management(in_table=reporting_layer,out_table=filt_layer,in_key_field_option="USE_KEY_FIELDS",in_key_field="#",in_field="#",where_clause=sql)
            arcpy.MakeFeatureLayer_management(reporting_layer, filt_layer, sql, "", "")

        inputCnt = int(arcpy.GetCount_management(in_rows=filt_layer).getOutput(0))
        if inputCnt == 0:
            pass
        else:

            if report_params['Type'].upper() == "AVERAGE":
                'If we want Speedy Intersect to adjust all the average fields, we need to enhance it to support multi count fields'
                result = split_average(reporting_areas=reporting_areas,
                                       reporting_areas_ID_field=reporting_areas_ID_field,
                                       reporting_layer=filt_layer,
                                       reporting_layer_field_map=field_map,
                                       code_exp=code_exp,
                                       use_arcmap_expression=useArcMapExpression,
                                       adjust_count=False,
                                       count_field_name='',
                                       report_areas_overlap=report_areas_overlap)
            else:
                'If we want Speedy Intersect to adjust all the average fields, we need to enhance it to support multi count fields'
                result = split_statistic(reporting_areas=reporting_areas,
                                         reporting_areas_ID_field=reporting_areas_ID_field,
                                         reporting_layer=filt_layer,
                                         reporting_layer_field_map=field_map,
                                         code_exp=code_exp,
                                         use_arcmap_expression=useArcMapExpression,
                                         adjust_count=False,
                                         count_field_name='',
                                         report_areas_overlap=report_areas_overlap)

            if 'layer' in result:

                report_copy = copy_report_data_schema(reporting_areas=reporting_areas,
                                                      reporting_areas_ID_field=reporting_areas_ID_field,
                                                      report_schema=report_schema,
                                                      report_result=report_result,
                                                      join_layer=result['layer'],
                                                      report_output_type=report_output_type)

                if report_params['Type'].upper() == "AVERAGE":
                    report_result = calculate_average_report_results(report_result=report_result,
                                                                     reporting_areas_ID_field=reporting_areas_ID_field,
                                                                     report_copy=report_copy,
                                                                     field_map=avg_field_map,
                                                                     report_date_field=report_date_field,
                                                                     report_ID_field=report_ID_field,
                                                                     average_field=result['field'],
                                                                     reportParam=report_params,
                                                                     config=datasources)
                else:
                    report_result = calculate_statistic_report_results(report_result=report_result,
                                                                       reporting_areas_ID_field=reporting_areas_ID_field,
                                                                       report_copy=report_copy,
                                                                       field_map=avg_field_map,
                                                                       report_date_field=report_date_field,
                                                                       report_ID_field=report_ID_field,
                                                                       statistic_field=result['field'],
                                                                       reportParam=report_params,
                                                                       config=datasources)
                print "%s was created or updated" % report_result
                deleteFC(in_datasets=[report_copy, result['layer']])


    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_average_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except (ReportToolsError), e:
        raise e
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "create_average_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        filt_layer = None
        reporting_layer = None
        field_map = None
        sql = None
        code_exp = None
        report_schema = None
        report_result = None
        report_date_field = None
        report_ID_field = None
        avg_field_map = None
        inputCnt = None
        result = None
        report_copy = None
        report_result = None

        del filt_layer
        del reporting_layer
        del field_map
        del sql
        del code_exp
        del report_schema
        del report_result
        del report_date_field
        del report_ID_field
        del avg_field_map
        del inputCnt
        del result
        del report_copy

        gc.collect()


# ----------------------------------------------------------------------
def calculate_load_results(feature_data,
                           feature_data_id_field,
                           join_table,
                           join_table_id_field,
                           report_date_field,
                           report_result,
                           field_map
                           ):
    _tempWorkspace = None
    _feature_data_layer = None
    _join_table_copy = None
    _joinedDataFull = None
    _pointsJoinedData = None
    _pointsJoinedDataFull = None
    joinTableDesc = None
    joinName = None
    featureDataDesc = None
    featureDataName = None
    fields = None
    tFields = None
    layerFlds = None
    new_row = None
    dt = None
    onlTm = None
    timeStr = None
    strOnlineTime = None
    try:
        _tempWorkspace = env.scratchGDB

        _feature_data_layer = Common.random_string_generator()
        _join_table_copy = Common.random_string_generator()
        _joinedDataFull = os.path.join(_tempWorkspace, _join_table_copy)

        _pointsJoinedData = Common.random_string_generator()
        _pointsJoinedDataFull = os.path.join(_tempWorkspace, _pointsJoinedData)
        # Process: Make Feature Layer
        if arcpy.Exists(dataset=feature_data) == False:
            return {"Error": feature_data + " Does not exist"}

        if arcpy.Exists(dataset=join_table) == False:
            return {"Error": join_table + " Does not exist"}

        arcpy.MakeFeatureLayer_management(in_features=feature_data, out_layer=_feature_data_layer, where_clause=None,
                                          workspace=None, field_info=None)

        # Process: Table to Table
        arcpy.TableToTable_conversion(join_table, _tempWorkspace, _join_table_copy, "", "#", "")

        # Process: Add Join
        arcpy.AddJoin_management(_feature_data_layer, feature_data_id_field, _joinedDataFull, join_table_id_field,
                                 "KEEP_COMMON")

        arcpy.FeatureClassToFeatureClass_conversion(_feature_data_layer, _tempWorkspace, _pointsJoinedData, "", "", "")

        joinTableDesc = arcpy.Describe(_joinedDataFull)
        joinName = str(joinTableDesc.name)

        featureDataDesc = arcpy.Describe(feature_data)
        featureDataName = str(featureDataDesc.name)

        try:
            arcpy.RemoveJoin_management(_feature_data_layer, joinName)
        except:
            pass

        fields = []
        tFields = []
        layerFlds = fieldsToFieldArray(featureclass=_pointsJoinedDataFull)

        for fld in field_map:
            if fld['FieldName'] in layerFlds:

                fields.append(fld['FieldName'])
            elif joinName + "_" + fld['FieldName'] in layerFlds:
                fld['FieldName'] = joinName + "_" + fld['FieldName']
                fields.append(fld['FieldName'])
            elif featureDataName + "_" + fld['FieldName'] in layerFlds:
                fld['FieldName'] = featureDataName + "_" + fld['FieldName']
                fields.append(fld['FieldName'])

        if len(fields) != len(field_map):
            print "Field Map length does not match fields in layer, exiting"
            return

        for fld in field_map:
            tFields.append(fld['TargetField'])

        tFields.append("SHAPE@")

        fields.append("SHAPE@")

        datefld = -1
        if report_date_field in tFields:
            datefld = tFields.index(report_date_field)
        elif report_date_field != '':
            tFields.append(report_date_field)

        with arcpy.da.InsertCursor(report_result, tFields) as icursor:
            strOnlineTime = Common.online_time_to_string(Common.local_time_to_online(), dateTimeFormat)

            with arcpy.da.SearchCursor(_pointsJoinedDataFull, fields) as scursor:
                for row in scursor:
                    new_row = list(row)
                    if datefld > -1:
                        try:
                            if isinstance(new_row[datefld], basestring):
                                dt = parse(new_row[datefld])
                            else:
                                dt = new_row[datefld]

                            onlTm = Common.local_time_to_online(dt)
                            timeStr = Common.online_time_to_string(onlTm, dateTimeFormat)
                            new_row[datefld] = timeStr
                        except:
                            new_row[datefld] = strOnlineTime
                    elif report_date_field != '':
                        new_row.append(strOnlineTime)
                    icursor.insertRow(new_row)
                del row
                del scursor
            del icursor

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_load_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_load_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        _feature_data_layer = None
        _join_table_copy = None
        _joinedDataFull = None
        _pointsJoinedData = None
        _pointsJoinedDataFull = None
        joinTableDesc = None
        joinName = None
        featureDataDesc = None
        featureDataName = None
        fields = None
        tFields = None
        layerFlds = None
        new_row = None
        dt = None
        onlTm = None
        timeStr = None
        strOnlineTime = None

        del _tempWorkspace
        del _feature_data_layer
        del _join_table_copy
        del _joinedDataFull
        del _pointsJoinedData
        del _pointsJoinedDataFull
        del joinTableDesc
        del joinName
        del featureDataDesc
        del featureDataName
        del fields
        del tFields
        del layerFlds
        del new_row
        del dt
        del onlTm
        del timeStr
        del strOnlineTime
        gc.collect()


# ----------------------------------------------------------------------
def split_average(reporting_areas,
                  reporting_areas_ID_field,
                  reporting_layer,
                  reporting_layer_field_map,
                  code_exp,
                  use_arcmap_expression=False,
                  adjust_count=False,
                  count_field_name='',
                  report_areas_overlap=True):
    _tempWorkspace = None
    _intersect = None
    sumstats = None
    age_field = None
    try:
        _tempWorkspace = env.scratchGDB

        _intersect = os.path.join(_tempWorkspace, Common.random_string_generator())
        sumstats = os.path.join(_tempWorkspace, Common.random_string_generator())

        # Process: Intersect Reporting Areas with Reporting Data to split them for accurate measurements
        # arcpy.Intersect_analysis(in_features="'"+ reporting_areas + "' #;'" + reporting_layer+ "' #",out_feature_class= _intersect,join_attributes="ALL",cluster_tolerance="#",output_type="INPUT")
        count_field_adjust = count_field_name if adjust_count == True else None
        gptools.speedyIntersect(fcToSplit=reporting_layer,
                                splitFC=reporting_areas,
                                fieldsToAssign=[reporting_areas_ID_field],
                                countField=count_field_adjust,
                                onlyKeepLargest=False,
                                outputFC=_intersect,
                                report_areas_overlap=report_areas_overlap)

        age_field = "statsfield"
        # Process: Add a field and calculate it with the groupings required for reporting.
        arcpy.AddField_management(in_table=_intersect, field_name=age_field, field_type="LONG", field_precision="",
                                  field_scale="", field_length="",
                                  field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED",
                                  field_domain="")

        if use_arcmap_expression:

            arcpy.CalculateField_management(in_table=_intersect, field=age_field,
                                            expression=code_exp,
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
        else:
            calc_field(inputDataset=_intersect, field_map=reporting_layer_field_map, code_exp=code_exp,
                       result_field=age_field)

        arcpy.Statistics_analysis(_intersect, out_table=sumstats, statistics_fields=age_field + " MEAN",
                                  case_field=reporting_areas_ID_field)

        deleteFC([_intersect])
        return {"layer": sumstats, "field": "MEAN_" + age_field}

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "split_average",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "split_average",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        _intersect = None

        del _tempWorkspace
        del _intersect

        gc.collect()


# ----------------------------------------------------------------------

def split_statistic(reporting_areas,
                    reporting_areas_ID_field,
                    reporting_layer,
                    reporting_layer_field_map,
                    code_exp,
                    use_arcmap_expression=False,
                    adjust_count=False,
                    count_field_name='',
                    report_areas_overlap=True):
    _tempWorkspace = None
    _intersect = None
    sumstats = None
    statsfield = None
    try:
        _tempWorkspace = env.scratchGDB

        _intersect = os.path.join(_tempWorkspace, Common.random_string_generator())
        sumstats = os.path.join(_tempWorkspace, Common.random_string_generator())

        # Process: Intersect Reporting Areas with Reporting Data to split them for accurate measurements
        # arcpy.Intersect_analysis(in_features=[reporting_areas, reporting_layer], out_feature_class=_intersect, join_attributes="ALL", cluster_tolerance="#", output_type="INPUT")
        count_field_adjust = count_field_name if adjust_count == True else None
        gptools.speedyIntersect(fcToSplit=reporting_layer,
                                splitFC=reporting_areas,
                                fieldsToAssign=[reporting_areas_ID_field],
                                countField=count_field_adjust,
                                onlyKeepLargest=False,
                                outputFC=_intersect,
                                report_areas_overlap=report_areas_overlap)

        statsfield = "statsfield"
        # Process: Add a field and calculate it with the groupings required for reporting.
        arcpy.AddField_management(in_table=_intersect, field_name=statsfield, field_type="LONG", field_precision="",
                                  field_scale="", field_length="",
                                  field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED",
                                  field_domain="")

        if use_arcmap_expression:

            arcpy.CalculateField_management(in_table=_intersect, field=statsfield,
                                            expression=code_exp,
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
        else:
            calc_field(inputDataset=_intersect, field_map=reporting_layer_field_map, code_exp=code_exp,
                       result_field=statsfield)

        statistics_fields = [[statsfield, s] for s in
                             ["SUM", "MEAN", "MIN", "MAX", "RANGE", "STD", "COUNT", "FIRST", "LAST"]]
        arcpy.Statistics_analysis(_intersect, out_table=sumstats, statistics_fields=statistics_fields,
                                  case_field=reporting_areas_ID_field)

        deleteFC([_intersect])
        return {"layer": sumstats, "field": statsfield}

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "split_statistic",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "split_statistics",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        _intersect = None

        del _tempWorkspace
        del _intersect

        gc.collect()


# ----------------------------------------------------------------------
def split_reclass(reporting_areas, reporting_areas_ID_field, reporting_layer, field_map, reclass_map,
                  classified_layer_field_name,
                  count_field_name, use_arcmap_expression=False, reclass_type='split',
                  adjust_count=False,
                  report_areas_overlap=True):
    _tempWorkspace = None
    _intersect = None
    flds = None
    val_fnd = None
    sql_state = None

    try:
        _tempWorkspace = env.scratchGDB

        _intersect = os.path.join(_tempWorkspace, Common.random_string_generator())
        _unique_name = Common.random_string_generator()

        reclassLayer = os.path.join(_tempWorkspace, _unique_name)

        if reclass_type == 'single':
            keep_largest = True
            # shapeBasedSpatialJoin(TargetLayer=reporting_layer, JoinLayer=reporting_areas, JoinResult=_intersect)
            count_field_adjust = count_field_name if adjust_count == True else None
            gptools.speedyIntersect(fcToSplit=reporting_layer,
                                    splitFC=reporting_areas,
                                    fieldsToAssign=[reporting_areas_ID_field],
                                    countField=count_field_adjust,
                                    onlyKeepLargest=keep_largest,
                                    outputFC=_intersect,
                                    report_areas_overlap=report_areas_overlap)
        else:
            keep_largest = False

            # Process: Intersect Reporting Areas with Reporting Data to split them for accurate measurements
            count_field_adjust = count_field_name if adjust_count == True else None
            gptools.speedyIntersect(fcToSplit=reporting_layer,
                                    splitFC=reporting_areas,
                                    fieldsToAssign=[reporting_areas_ID_field],
                                    countField=count_field_adjust,
                                    onlyKeepLargest=keep_largest,
                                    outputFC=_intersect,
                                    report_areas_overlap=report_areas_overlap)
            # arcpy.Intersect_analysis(in_features="'"+ reporting_areas + "' #;'" + reporting_layer+ "' #",out_feature_class= _intersect,join_attributes="ALL",cluster_tolerance="#",output_type="INPUT")
        # Process: Add a field and calculate it with the groupings required for reporting. .
        # Process: Create Reclass Feature Class
        desc = arcpy.Describe(_intersect)

        arcpy.CreateFeatureclass_management(_tempWorkspace, _unique_name, str(desc.shapeType).upper(), "", "DISABLED",
                                            "DISABLED", _intersect, "", "0", "0", "0")
        del desc
        arcpy.AddField_management(in_table=reclassLayer, field_name=classified_layer_field_name, field_type="TEXT",
                                  field_precision="", field_scale="", field_length="",
                                  field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED",
                                  field_domain="")
        idFld = arcpy.ListFields(dataset=reporting_areas, wild_card=reporting_areas_ID_field)
        if len(idFld) > 0:
            arcpy.AddField_management(in_table=reclassLayer, field_name=reporting_areas_ID_field,
                                      field_type=idFld[0].type,
                                      field_precision=idFld[0].precision,
                                      field_scale=idFld[0].scale,
                                      field_length=idFld[0].length,
                                      field_alias=idFld[0].aliasName,
                                      field_is_nullable=idFld[0].isNullable,
                                      field_is_required=idFld[0].required,
                                      field_domain=idFld[0].domain)

        else:
            arcpy.AddField_management(in_table=reclassLayer, field_name=reporting_areas_ID_field, field_type="TEXT",
                                      field_precision="", field_scale="", field_length="",
                                      field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED",
                                      field_domain="")

        reclassFlds = []
        reclassFlds.append(reporting_areas_ID_field)
        reclassFlds.append("SHAPE@")
        reclassFlds.append(classified_layer_field_name)
        flds = []

        for fld in field_map:
            flds.append(fld['FieldName'])
        flds.append(reporting_areas_ID_field)
        flds.append("SHAPE@")

        countFieldAdded = False
        countField = arcpy.ListFields(reporting_layer, count_field_name)

        if len(countField) > 0:
            arcpy.AddField_management(in_table=reclassLayer, field_name=countField[0].name,
                                      field_type=countField[0].type,
                                      field_precision=countField[0].precision, field_scale=countField[0].scale,
                                      field_length=countField[0].length,
                                      field_alias=countField[0].aliasName, field_is_nullable=countField[0].isNullable,
                                      field_is_required=countField[0].required, field_domain="")
            countFieldAdded = True
            reclassFlds.append(countField[0].name)
            flds.append(countField[0].name)

        selectLayer = "selectLayer"
        if use_arcmap_expression:
            for field in reclass_map:
                sql_state = field['Expression']
                try:
                    arcpy.MakeFeatureLayer_management(in_features=_intersect,
                                                      out_layer=selectLayer,
                                                      where_clause=sql_state,
                                                      workspace=None,
                                                      field_info=None)
                except arcpy.ExecuteError:
                    line, filename, synerror = trace()
                    raise ReportToolsError({
                        "function": "create_reclass_report: %s" % split_reclass,
                        "line": line,
                        "filename": filename,
                        "synerror": synerror,
                        "arcpyError": arcpy.GetMessages(2),
                    })
                reccount = int(arcpy.GetCount_management(selectLayer)[0])
                # print "%s records found to match %s" % (field["FieldName"],str(reccount))
                if reccount > 0:
                    with arcpy.da.InsertCursor(reclassLayer, reclassFlds) as irows:
                        with arcpy.da.SearchCursor(selectLayer, flds) as srows:
                            for row in srows:
                                if countFieldAdded:
                                    irows.insertRow((row[len(flds) - 3], row[len(flds) - 2], field["FieldName"],
                                                     row[len(flds) - 1]))
                                else:
                                    irows.insertRow((row[len(flds) - 2], row[len(flds) - 1], field["FieldName"]))
                                del row
                        del srows

                    del irows
        else:

            with arcpy.da.InsertCursor(reclassLayer, reclassFlds) as irows:
                with arcpy.da.SearchCursor(_intersect, flds) as srows:
                    for row in srows:

                        for field in reclass_map:
                            sql_state = field['Expression']
                            try:

                                for i in range(len(field_map)):
                                    sql_state = sql_state.replace(field_map[i]['Expression'], str(row[i]))

                                if eval(sql_state) == True:
                                    if countFieldAdded:
                                        irows.insertRow((row[len(flds) - 3], row[len(flds) - 2], field["FieldName"],
                                                         row[len(flds) - 1]))
                                    else:
                                        irows.insertRow((row[len(flds) - 2], row[len(flds) - 1], field["FieldName"]))


                            except Exception, e:
                                print "Warning: %s is not valid" % str(sql_state)

                    del row
                del srows

            del irows
        # print "done update cursors"
        deleteFC([_intersect])
        return reclassLayer

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "split_reclass",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except ReportToolsError, e:
        raise e

    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "split_reclass",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        flds = None
        val_fnd = None
        sql_state = None

        del _tempWorkspace
        del flds
        del val_fnd
        del sql_state

        gc.collect()


# ----------------------------------------------------------------------
def classified_pivot(classified_layer, classified_layer_field_name, reporting_areas_ID_field, count_field,
                     summary_fields=''):
    _tempWorkspace = None
    _freq = None
    _pivot = None

    try:
        _tempWorkspace = env.scratchGDB

        _freq = os.path.join(_tempWorkspace, Common.random_string_generator())
        _pivot = os.path.join(_tempWorkspace, Common.random_string_generator())

        if not count_field in summary_fields and count_field != 'FREQUENCY':
            summary_fields = count_field if summary_fields == '' else summary_fields + ";" + count_field

        arcpy.Frequency_analysis(in_table=classified_layer, out_table=_freq,
                                 frequency_fields=reporting_areas_ID_field + ';' + classified_layer_field_name,
                                 summary_fields=summary_fields)

        arcpy.PivotTable_management(_freq, reporting_areas_ID_field, classified_layer_field_name, count_field, _pivot)
        deleteFC([_freq])
        return _pivot
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "classified_pivot",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "classified_pivot",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        _freq = None

        del _tempWorkspace
        del _freq

        gc.collect()


# ----------------------------------------------------------------------
def copy_report_data_schema(reporting_areas,
                            reporting_areas_ID_field,
                            report_schema,
                            report_result,
                            join_layer,
                            report_output_type):
    _tempWorkspace = None
    _reportCopy = None
    final_report = None

    try:
        _tempWorkspace = env.scratchGDB
        _reportCopy = Common.random_string_generator()
        final_report = os.path.join(_tempWorkspace, _reportCopy)

        # Process: Create a copy of the Reporting Areas for the summary info join
        fm_id = arcpy.FieldMap()
        fms = arcpy.FieldMappings()

        # Add fields to their corresponding FieldMap objects
        fm_id.addInputField(reporting_areas, reporting_areas_ID_field)

        # Set the output field properties for both FieldMap objects
        type_name = fm_id.outputField
        type_name.name = reporting_areas_ID_field
        fm_id.outputField = type_name
        fms.addFieldMap(fm_id)
        arcpy.FeatureClassToFeatureClass_conversion(in_features=reporting_areas,
                                                    out_path=_tempWorkspace,
                                                    out_name=_reportCopy,
                                                    field_mapping=fms)

        # Process: Join the Areas to the pivot table to get a count by area
        arcpy.JoinField_management(final_report, reporting_areas_ID_field, join_layer, reporting_areas_ID_field, "#")

        # Process: Create a copy of the report layer
        # if not os.path.isabs(report_result):
        # report_result =os.path.abspath( report_result)

        # if not os.path.isabs(report_schema):
        # report_schema =os.path.abspath( report_schema)

        # New flag for weather or not to append data to exist results or clear it out.
        if report_output_type.upper() == "APPEND":
            if arcpy.Exists(report_result) == False:
                if arcpy.Exists(report_schema) == False:
                    raise ReportToolsError({
                        "function": "copy_report_data_schema",
                        "line": 990,
                        "filename": 'reporttools',
                        "synerror": "%s could not be located" % report_schema
                    }
                    )
                className = os.path.basename(report_result)
                layerPath = os.path.dirname(report_result)
                arcpy.FeatureClassToFeatureClass_conversion(in_features=report_schema,
                                                            out_path=layerPath,
                                                            out_name=className,
                                                            where_clause=None,
                                                            field_mapping=None,
                                                            config_keyword=None)
                # arcpy.Copy_management(report_schema,report_result,"FeatureClass")
        elif report_output_type.upper() == "UPDATE":
            if not arcpy.Exists(dataset=report_result):
                if arcpy.Exists(report_schema) == False:
                    raise ReportToolsError({
                        "function": "copy_report_data_schema",
                        "line": 990,
                        "filename": 'reporttools',
                        "synerror": "%s could not be located" % report_schema
                    }
                    )
                className = os.path.basename(report_result)
                layerPath = os.path.dirname(report_result)

                arcpy.FeatureClassToFeatureClass_conversion(in_features=report_schema,
                                                            out_path=layerPath,
                                                            out_name=className,
                                                            where_clause=None,
                                                            field_mapping=None,
                                                            config_keyword=None)
        else:
            # arcpy.Copy_management(report_schema,report_result,"FeatureClass")
            className = os.path.basename(report_result)
            layerPath = os.path.dirname(report_result)

            arcpy.FeatureClassToFeatureClass_conversion(in_features=report_schema,
                                                        out_path=layerPath,
                                                        out_name=className,
                                                        where_clause=None,
                                                        field_mapping=None,
                                                        config_keyword=None)
        return final_report
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "copy_report_data_schema",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "copy_report_data_schema",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        _reportCopy = None

        del _tempWorkspace
        del _reportCopy

        gc.collect()


# ----------------------------------------------------------------------
def calculate_average_report_results(report_result, reporting_areas_ID_field, report_copy, field_map,
                                     report_date_field, report_ID_field, average_field, reportParam, config):
    fields = None
    strOnlineTime = None
    search_fields = None
    newrow = None
    try:
        fields = []
        for fld in field_map:
            fields.append(fld['FieldName'])

        fields.append(report_ID_field)
        fields.append(report_date_field)
        fields.append("SHAPE@")

        # strOnlineTime = Common.online_time_to_string(Common.local_time_to_online(),dateTimeFormat)
        strLocalTime = datetime.datetime.now().strftime(dateTimeFormat)
        search_fields = [average_field, reporting_areas_ID_field, "SHAPE@"]

        with arcpy.da.InsertCursor(report_result, fields) as irows:
            with arcpy.da.SearchCursor(report_copy, search_fields) as srows:
                for row in srows:
                    newrow = []
                    for fld in field_map:
                        try:
                            if '{Value}' in fld['Expression']:
                                newrow.append(
                                    eval(fld["Expression"].replace("{Value}", str(Common.noneToValue(row[0], 0.0)))))
                            else:
                                newrow.append(eval(fld["Expression"].replace("{PreCalcExpressionValue}",
                                                                             str(Common.noneToValue(row[0], 0.0)))))
                        except Exception:
                            newrow.append(None)
                    newrow.append(row[1])
                    newrow.append(strLocalTime)
                    newrow.append(row[2])

                    irows.insertRow(tuple(newrow))
            del srows
        del irows

        # The current recordset that is completed, send it to a merged feature for CSV export process layer.
        mergeAllReports(reportLayer=report_result, report=reportParam, config=config)

        return report_result
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_average_report_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_average_report_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        fields = None
        strOnlineTime = None
        search_fields = None
        newrow = None

        del fields
        del strOnlineTime
        del search_fields
        del newrow

        gc.collect()


# ----------------------------------------------------------------------
def calculate_statistic_report_results(report_result, reporting_areas_ID_field, report_copy, field_map,
                                       report_date_field, report_ID_field, statistic_field, reportParam, config):
    fields = None
    strOnlineTime = None
    search_fields = None
    newrow = None
    try:

        def dictLookup(expr, d):
            """replace values in string surrounded in {} with a dictionary key,value pair
               attempt to evaluate expression
               expr - str to replace
               d - dict
            """
            for k, v in d.items():
                expr = expr.replace("{{{}}}".format(k), str(Common.noneToValue(v, 0.0)))
            try:
                return eval(expr)
            except:
                line, filename, synerror = trace()
                raise ReportToolsError({
                    "function": "dictLookup",
                    "line": line,
                    "filename": filename,
                    "synerror": synerror,
                    "expr": expr})

        # Summary statistics creates a table with fields named {statistic}_{fname}, e.g., "MEAN_age"
        stats = ["SUM", "MEAN", "MIN", "MAX", "RANGE", "STD", "COUNT", "FIRST", "LAST"]
        stats_fields = ["{}_{}".format(s, statistic_field) for s in stats] + [reporting_areas_ID_field, "SHAPE@"]
        keys = stats + ["ID", "SHAPE"]

        # srows is a list of dictionaries mapping statistics to that row's field value
        srows = [{s: r for s, r in zip(keys, row)} for row in arcpy.da.SearchCursor(report_copy, stats_fields)]

        # strOnlineTime = Common.online_time_to_string(Common.local_time_to_online(),dateTimeFormat)
        strLocalTime = datetime.datetime.now().strftime(dateTimeFormat)

        fields = [fld['FieldName'] for fld in field_map]
        fields.append(report_ID_field)
        fields.append(report_date_field)
        fields.append("SHAPE@")

        with arcpy.da.InsertCursor(report_result, fields) as irows:
            for row in srows:
                newrow = []
                for fld in field_map:
                    try:
                        newrow.append(dictLookup(fld["Expression"], row))
                    except:
                        newrow.append(None)
                newrow.append(row["ID"])
                newrow.append(strLocalTime)
                newrow.append(row["SHAPE"])

                irows.insertRow(tuple(newrow))

        # The current recordset that is completed, send it to a merged feature for CSV export process layer.
        mergeAllReports(reportLayer=report_result, report=reportParam, config=config)

        return report_result
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_statistic_report_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_statistic_report_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        fields = None
        strOnlineTime = None
        search_fields = None
        newrow = None

        del fields
        del strOnlineTime
        del search_fields
        del newrow

        gc.collect()


# ----------------------------------------------------------------------
def calculate_report_results(report_result,
                             reporting_areas_ID_field, report_copy,
                             reclass_map,
                             report_date_field,
                             report_ID_field,
                             exp, reportParam,
                             config, use_arcmap_expression=False,
                             report_output_type='Overwrite',
                             delete_append_sql=None):
    try:

        reporting_areas_Date_field = 'tempreportdate1'
        arcpy.AddField_management(in_table=report_copy, field_name=reporting_areas_Date_field, field_type="DATE",
                                  field_precision="", field_scale="", field_length="", field_alias="",
                                  field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")

        # strOnlineTime = Common.online_time_to_string(Common.local_time_to_online(),dateTimeFormat)
        strLocalTime = datetime.datetime.now().strftime(dateTimeFormat)

        reclass_fields = []
        copyFields = []

        fieldBaseFinalExp = {}
        for fld in reclass_map:
            reclass_fields.append(fld['FieldName'])
            if "FinalResultExpression" in fld:
                fieldBaseFinalExp[fld['FieldName']] = fld['FinalResultExpression']

        reportDataFieldNames = [f.name for f in arcpy.ListFields(report_copy)]
        for reclass_field in reclass_fields:
            if not reclass_field in reportDataFieldNames:
                arcpy.AddField_management(in_table=report_copy, field_name=reclass_field, field_type="TEXT",
                                          field_precision="", field_scale="", field_length="", field_alias="",
                                          field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED",
                                          field_domain="")

        reportDataFields = arcpy.ListFields(report_copy)

        resultFields = [f.name for f in arcpy.ListFields(report_result)]

        fms = arcpy.FieldMappings()
        for fld in reportDataFields:

            if fld.name == reporting_areas_Date_field and report_date_field != '':
                fm = arcpy.FieldMap()

                fm.addInputField(report_copy, fld.name)
                outField = fm.outputField
                outField.name = report_date_field
                fm.outputField = outField
                fms.addFieldMap(fm)

                copyFields.append((fld.name, report_date_field))
            elif fld.name == reporting_areas_ID_field:
                fm = arcpy.FieldMap()
                fm.addInputField(report_copy, fld.name)
                outField = fm.outputField
                outField.name = report_ID_field
                fm.outputField = outField
                fms.addFieldMap(fm)
                # copyFields.append((fld.name,reporting_areas_ID_field ))
            elif fld.name in resultFields and fld.type != 'OID' and fld.type != 'Geometry':
                fm = arcpy.FieldMap()
                fm.addInputField(report_copy, fld.name)
                outField = fm.outputField
                outField.name = fld.name
                fm.outputField = outField
                fms.addFieldMap(fm)
                if fld.name in reclass_fields:
                    copyFields.append((fld.name, fld.name))

                    # className = os.path.basename(report_result)
                    # layerPath = os.path.dirname(report_result)

                    # with arcpy.da.Editor(layerPath):
                    # for fld in reclass_fields:

                    # outLayer = "outLayer%s" % fld
                    # where = '%s IS NULL' % fld
                    # arcpy.MakeFeatureLayer_management(report_result, outLayer, where)
                    # arcpy.CalculateField_management(outLayer, fld['FieldName'],
                    # '0', "PYTHON_9.3")

                    # arcpy.CalculateField_management(report_result, reporting_areas_Date_field,
                    # 'time.strftime(\'%d/%m/%Y %H:%M\')', "PYTHON_9.3")

        reclass_fields.append(reporting_areas_Date_field)
        with arcpy.da.UpdateCursor(report_copy, reclass_fields) as urows:
            for row in urows:
                for u in range(len(reclass_fields) - 1):
                    if reclass_fields[u] in fieldBaseFinalExp:
                        finalExp = fieldBaseFinalExp[reclass_fields[u]]
                        finalExpOrig = finalExp
                        for k in range(len(reclass_fields) - 1):
                            finalFieldVal = reclass_fields[k]
                            if "{" + finalFieldVal + "}" in finalExp:
                                finalExp = finalExp.replace("{" + finalFieldVal + "}",
                                                            str(Common.noneToValue(row[k], 0)))
                        try:
                            finalExp = finalExp.replace("{Value}", str(Common.noneToValue(row[u], 0)))
                            finalExp = finalExp.replace("{ReclassField}", str(Common.noneToValue(row[u], 0)))
                            row[u] = eval(finalExp)
                        except:
                            line, filename, synerror = trace()
                            # print "Warning: Evaluating Final Expression {3} with values {0} for the field {1} failed with the following error: {2}".format(finalExp,reclass_fields[u],synerror,finalExpOrig)
                            row[u] = Common.noneToValue(row[u], 0)

                    else:
                        if '{Value}' in exp:
                            row[u] = eval(exp.replace("{Value}", str(Common.noneToValue(row[u], 0))))
                        else:
                            row[u] = eval(exp.replace("{ReclassField}", str(Common.noneToValue(row[u], 0))))

                row[len(reclass_fields) - 1] = strLocalTime

                urows.updateRow(row)
            del urows

        if report_output_type.upper() == "UPDATE":

            JoinAndCalc(inputDataset=report_result,
                        inputJoinField=report_ID_field,
                        joinTable=report_copy,
                        joinTableJoinField=reporting_areas_ID_field,
                        copyFields=copyFields,
                        joinType="KEEP_COMMON",
                        inputFilter=delete_append_sql)
            # arcpy.MakeFeatureLayer_management(in_features=report_result,
            # out_layer=delfromreport,
            # where_clause=delete_append_sql,
            # workspace=None,
            # field_info=None)
            # arcpy.DeleteFeatures_management(in_features=delfromreport)
        else:
            arcpy.Append_management(report_copy, report_result, "NO_TEST", fms, "")

        # The current recordset that is completed, send it to a merged feature for CSV export process layer.
        mergeAllReports(reportLayer=report_result, report=reportParam, config=config)

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_report_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except ReportToolsError, e:
        raise e
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_report_results",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )

    finally:

        gc.collect()


# ----------------------------------------------------------------------
def validate_schema_map(report_schema, reclass_map, report_date_field, report_ID_field):
    try:
        valid = True
        fieldList = arcpy.ListFields(report_schema)

        layer_fields = []
        for field in fieldList:
            layer_fields.append(field.name)

        for fld in reclass_map:
            if not fld['FieldName'] in layer_fields:
                print "%s does not exist in %s" % (fld['FieldName'], report_schema)
                valid = False
        if report_date_field == '':
            print "Warning: Report Date not set in %s" % (report_schema)
        elif not report_date_field in layer_fields:
            print "%s (Report Date Field) does not exist in %s" % (report_date_field, report_schema)
            valid = False
        if not report_ID_field in layer_fields:
            print "%s (ID Field) does not exist in %s" % (report_ID_field, report_schema)
            valid = False

        if valid == False:
            raise ReportToolsError({
                "function": "validate_schema_map",
                "line": 1454,
                "filename": 'reporttools',
                "synerror": "%s does not contain all the fields contained in the config" % report_schema
            })
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "validate_schema_map",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        })
    except ReportToolsError, e:
        raise e
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "validate_schema_map",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })


# ----------------------------------------------------------------------
def validate_id_field(reporting_areas, report_ID_field):
    try:

        OIDField = arcpy.ListFields(dataset=reporting_areas, field_type='OID')
        if len(OIDField) > 0 and OIDField[0].name == report_ID_field:
            line, filename, synerror = trace()
            raise ReportToolsError({
                "function": "validate_id_field",
                "line": line,
                "filename": filename,
                "synerror": "OBJECTID cannot be used for ID field",
            })
        globalIDField = arcpy.ListFields(dataset=reporting_areas, field_type='GlobalID')
        _tempWorkspace = env.scratchGDB
        if len(globalIDField) > 0 and globalIDField[0].name == report_ID_field:
            desc = arcpy.Describe(value=reporting_areas)
            _globalIDCopy_name = Common.random_string_generator()
            globalIDCopy = os.path.join(_tempWorkspace, _globalIDCopy_name)
            reportCopy = arcpy.CreateFeatureclass_management(out_path=_tempWorkspace,
                                                             out_name=_globalIDCopy_name,
                                                             geometry_type=desc.shapeType,
                                                             template=reporting_areas,
                                                             has_m="DISABLED",
                                                             has_z="DISABLED",
                                                             spatial_reference=desc.spatialReference)

            # arcpy.FeatureClassToFeatureClass_conversion(reporting_areas, _tempWorkspace, _globalIDCopy_name)
            globalIDFldName = "GLOBALIDCopy_12_1"
            arcpy.AddField_management(in_table=globalIDCopy, field_name=globalIDFldName, field_type="GUID",
                                      field_precision="", field_scale="", field_length="",
                                      field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED",
                                      field_domain="")

            fms = arcpy.FieldMappings()
            reportDataFields = arcpy.ListFields(dataset=reporting_areas)
            for fld in reportDataFields:
                if (fld.type <> "OID" and fld.type <> "GlobalID" and fld.type <> "Geometry"):
                    try:
                        if (fld.name <> desc.areaFieldName and fld.name <> desc.lengthFieldName):
                            fm = arcpy.FieldMap()

                            fm.addInputField(globalIDCopy, fld.name)
                            outField = fm.outputField
                            outField.name = fld.name
                            fm.outputField = outField
                            fms.addFieldMap(fm)
                    except:
                        pass

            fm.addInputField(globalIDCopy, report_ID_field)
            outField = fm.outputField
            outField.name = globalIDFldName
            fm.outputField = outField
            fms.addFieldMap(fm)
            arcpy.Append_management(reporting_areas, globalIDCopy, "NO_TEST", fms, "")

            # arcpy.CalculateField_management(globalIDCopy, globalIDFldName,
            #                                "!" + report_ID_field + "!", "PYTHON_9.3")

            return {"ReportLayer": globalIDCopy,
                    "IDField": globalIDFldName}

        else:
            return None
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "validate_id_field",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        })
    except ReportToolsError, e:
        raise e
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "validate_id_field",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })


# ----------------------------------------------------------------------
def copy_empty_report(reporting_areas, reporting_areas_ID_field, report_schema, report_result, reclass_map,
                      report_date_field, report_ID_field, reportParam, config, report_output_type):
    _tempWorkspace = None
    _reportCopy = None
    _final_report = None
    appendString = None
    strOnlineTime = None
    fields = None

    try:
        _tempWorkspace = env.scratchGDB

        _reportCopy = Common.random_string_generator()

        _final_report = os.path.join(_tempWorkspace, _reportCopy)

        # Process: Create a copy of the Reporting Areas for the summary info join
        arcpy.FeatureClassToFeatureClass_conversion(reporting_areas, _tempWorkspace, _reportCopy, "",
                                                    reporting_areas_ID_field + ' "' + reporting_areas_ID_field + '" true true false 50 Text 0 0 ,First,#,' + reporting_areas + ',' + reporting_areas_ID_field + ',-1,-1',
                                                    "")

        className = os.path.basename(report_result)
        layerPath = os.path.dirname(report_result)
        bUseWhere = True

        if report_output_type.upper() == "APPEND":
            if arcpy.Exists(report_result) == False:
                if arcpy.Exists(report_schema) == False:
                    raise ReportToolsError({
                        "function": "copy_report_data_schema",
                        "line": 990,
                        "filename": 'reporttools',
                        "synerror": "%s could not be located" % report_schema
                    }
                    )
                arcpy.FeatureClassToFeatureClass_conversion(in_features=report_schema,
                                                            out_path=layerPath,
                                                            out_name=className,
                                                            where_clause=None,
                                                            field_mapping=None,
                                                            config_keyword=None)
                appendString = report_ID_field + " \"\" true true false 80 string 0 0 ,First,#," + _final_report + "," + reporting_areas_ID_field + ",-1,-1;"

                arcpy.Append_management(_final_report, report_result, "NO_TEST", appendString, "")
                # arcpy.Copy_management(report_schema,report_result,"FeatureClass")
        elif report_output_type.upper() == "UPDATE":
            bUseWhere = False


        else:

            arcpy.FeatureClassToFeatureClass_conversion(in_features=report_schema,
                                                        out_path=layerPath,
                                                        out_name=className,
                                                        where_clause=None,
                                                        field_mapping=None,
                                                        config_keyword=None)
            appendString = report_ID_field + " \"\" true true false 80 string 0 0 ,First,#," + _final_report + "," + reporting_areas_ID_field + ",-1,-1;"

            arcpy.Append_management(_final_report, report_result, "NO_TEST", appendString, "")

        # arcpy.Copy_management(report_schema,report_result,"FeatureClass")



        # strOnlineTime = Common.online_time_to_string(Common.local_time_to_online(),dateTimeFormat)
        # strLocalTime = datetime.datetime.now().strftime(dateTimeFormat)

        fields = []
        with arcpy.da.Editor(layerPath):
            for fld in reclass_map:
                # fields.append(fld['FieldName'])
                outLayer = "outLayer%s" % fld['FieldName']
                if bUseWhere:
                    where = '%s IS NULL' % fld['FieldName']
                else:
                    where = None
                arcpy.MakeFeatureLayer_management(report_result, outLayer, where)
                arcpy.CalculateField_management(outLayer, fld['FieldName'],
                                                '0', "PYTHON_9.3")

            arcpy.CalculateField_management(report_result, report_date_field,
                                            'time.strftime(\'%d/%m/%Y %H:%M\')', "PYTHON_9.3")
            # fields.append(report_date_field)

            # Start an edit session. Must provide the worksapce.
            # edit = arcpy.da.Editor(layerPath)

            # Edit session is started without an undo/redo stack for versioned data
            #  (for second argument, use False for unversioned data)
            # edit.startEditing(False, False)

            # Start an edit operation
            # edit.startOperation()

            # with arcpy.da.UpdateCursor(report_result,fields) as urows:
            # for row in urows:
            # for u in range(len(fields) - 1):
            # row[u]= str(Common.noneToValue( row[u],0.0))


            # row[len(fields)-1] = strOnlineTime
            # urows.updateRow(row)
            # del urows
            # Stop the edit operation.
        # edit.stopOperation()

        # Stop the edit session and save the changes
        # edit.stopEditing(True)
        # The current recordset that is completed, send it to a merged feature for CSV export process layer.
        mergeAllReports(reportLayer=report_result, report=reportParam, config=config)

        deleteFC([_final_report])
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "copy_empty_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "copy_empty_report",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        _reportCopy = None
        _final_report = None
        appendString = None
        strOnlineTime = None
        fields = None

        del _tempWorkspace
        del _reportCopy
        del _final_report
        del appendString
        del strOnlineTime
        del fields

        gc.collect()


# ----------------------------------------------------------------------
def shapeBasedSpatialJoin(TargetLayer, JoinLayer, JoinResult):
    _tempWorkspace = None
    _targetCopyName = None
    _targetCopy = None
    _areaFieldName = None
    _idenResultLayer = None

    layDetails = None
    _lenAreaFld = None

    try:
        if not arcpy.Exists(TargetLayer):
            raise ValueError(TargetLayer + " does not exist")
        if not arcpy.Exists(JoinLayer):
            raise ValueError(JoinLayer + " does not exist")
        # Local variables:
        _tempWorkspace = env.scratchGDB

        _targetCopyName = Common.random_string_generator()
        _targetCopy = os.path.join(_tempWorkspace, _targetCopyName)
        # JoinResult = os.path.join(_tempWorkspace ,random_string_generator())
        _areaFieldName = Common.random_string_generator(size=12)
        _idenResultLayer = "polyIdenLayer"

        _lenAreaFld = "SHAPE_Area"

        layDetails = arcpy.Describe(TargetLayer)
        if layDetails.shapeType == "Polygon":
            _lenAreaFld = "Shape_Area"
        elif layDetails.shapeType == "Polyline":
            _lenAreaFld = "Shape_Length"
        else:
            return ""
        arcpy.FeatureClassToFeatureClass_conversion(TargetLayer, _tempWorkspace, _targetCopyName, "#", "", "#")
        # Process: Copy
        # Process: Add Field
        arcpy.AddField_management(_targetCopy, _areaFieldName, "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")

        # Process: Calculate Field
        arcpy.CalculateField_management(_targetCopy, _areaFieldName, "!" + _lenAreaFld + "!", "PYTHON_9.3", "")

        # Process: Identity
        arcpy.Identity_analysis(_targetCopy, JoinLayer, JoinResult, "ALL", "", "NO_RELATIONSHIPS")

        # Process: Make Feature Layer
        arcpy.MakeFeatureLayer_management(JoinResult, _idenResultLayer, "", "", "")

        # Process: Select Layer By Attribute
        arcpy.SelectLayerByAttribute_management(_idenResultLayer, "NEW_SELECTION",
                                                _lenAreaFld + " < .5 *  " + _areaFieldName)

        # Process: Delete Features
        arcpy.DeleteFeatures_management(_idenResultLayer)

        deleteFC([_targetCopy])

        return JoinResult
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "shapeBasedSpatialJoin",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "shapeBasedSpatialJoin",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        _tempWorkspace = None
        _targetCopyName = None
        _targetCopy = None
        _areaFieldName = None
        _idenResultLayer = None

        layDetails = None
        _lenAreaFld = None

        del _tempWorkspace
        del _targetCopyName
        del _targetCopy
        del _areaFieldName
        del _idenResultLayer

        del layDetails
        del _lenAreaFld

        gc.collect()


# ----------------------------------------------------------------------
def JoinAndCalc(inputDataset, inputJoinField, joinTable, joinTableJoinField, copyFields, joinType="KEEP_ALL",
                inputFilter="None"):
    inputLayer = None
    joinTableDesc = None
    joinName = None
    removeJoin = None
    tz = None
    dateExp = None
    exp = None

    try:

        inputLayer = "inputLayer"
        if inputFilter == '':
            inputFilter = None
        arcpy.MakeFeatureLayer_management(in_features=inputDataset,
                                          out_layer=inputLayer,
                                          where_clause=inputFilter)

        joinTableDesc = arcpy.Describe(joinTable)
        joinName = str(joinTableDesc.name)
        arcpy.AddJoin_management(inputLayer, inputJoinField, joinTable, joinTableJoinField, joinType)
        removeJoin = True

        tz = time.timezone  # num of seconds to add to GMT based on current TimeZone
        workspace = os.path.dirname(inputDataset)
        # edit = arcpy.da.Editor(workspace)
        # edit.startEditing(False, True)
        # edit.startOperation()
        with arcpy.da.Editor(workspace) as edit:
            for copyField in copyFields:
                if len(copyField) == 3:
                    dateExp = "import time\\nimport datetime\\nfrom time import mktime\\nfrom datetime import datetime\\ndef calc(dt):\\n  return datetime.fromtimestamp(mktime(time.strptime(str(dt), '" + str(
                        copyField[2]) + "')) +  time.timezone)"
                    exp = 'calc(!' + joinName + '.' + copyField[0] + '!)'
                    arcpy.CalculateField_management(inputLayer, copyField[1], exp, 'PYTHON_9.3', dateExp)

                else:
                    arcpy.CalculateField_management(inputLayer, copyField[1], '!' + joinName + '.' + copyField[0] + '!',
                                                    "PYTHON_9.3", "")

                print copyField[1] + " Calculated from " + copyField[0]

            arcpy.RemoveJoin_management(inputLayer, joinName)
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "JoinAndCalc",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "JoinAndCalc",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        inputLayer = None
        joinTableDesc = None
        joinName = None
        removeJoin = None
        tz = None
        dateExp = None
        exp = None

        del inputLayer
        del joinTableDesc
        del joinName
        del removeJoin
        del tz
        del dateExp
        del exp


# ----------------------------------------------------------------------
def fieldsToFieldArray(featureclass):
    """fieldsToFieldArray(featureclass)

       Converts fields to a list

         featureclass(String):
       The specified feature class or table whose fields will be returned.

    """
    fieldList = None
    try:
        fieldList = arcpy.ListFields(featureclass)
        returnFields = []
        for field in fieldList:
            returnFields.append(field.name)

        return returnFields
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "fieldsToFieldArray",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        fieldList = None

        del fieldList

        gc.collect()


# ----------------------------------------------------------------------
def FieldExist(featureclass, fieldNames):
    """FieldExist(dataset, [fieldNames])

       Determines if the array of fields exist in the dataset

         dataset(String):
       The specified feature class or table whose indexes will be returned.

         fieldNames{Array}:
       The the array of field name to verify existance."""
    fieldList = None
    fndCnt = None

    try:
        fieldList = arcpy.ListFields(featureclass)
        fndCnt = 0
        for field in fieldList:
            if field.name in fieldNames:
                fndCnt = fndCnt + 1

            if fndCnt > 0:
                return True
            del field
        if fndCnt != len(fieldNames):
            return False

    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "FieldExist",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        }
        )
    finally:
        fieldList = None
        fndCnt = None

        del fieldList
        del fndCnt

        gc.collect()


# ----------------------------------------------------------------------
def calc_field(inputDataset, field_map, code_exp, result_field):
    res = None
    sqlState = None
    replaceValList = None
    newList = None

    try:

        replaceValList = []
        newList = []
        for fld in field_map:
            newList.append(fld['FieldName'])
            replaceValList.append(fld['ReplaceValue'])
        newList.append(result_field)
        with arcpy.da.UpdateCursor(inputDataset, newList) as cursor:

            for row in cursor:
                sqlState = code_exp
                try:
                    for i in range(0, len(replaceValList)):
                        sqlState = sqlState.replace(replaceValList[i], str(row[i]))

                    res = eval(sqlState)
                    row[len(newList) - 1] = res
                    cursor.updateRow(row)

                except Exception:
                    cursor.deleteRow()
            del row
            del cursor
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "JoinAndCalc",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calc_field",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })
    finally:
        res = None
        sqlState = None
        replaceValList = None
        newList = None

        del res
        del sqlState
        del replaceValList
        del newList

        gc.collect()


# ----------------------------------------------------------------------
def calculate_age_field(inputDataset, field, result_field):
    newList = None
    try:

        newList = [field, result_field]
        with arcpy.da.UpdateCursor(inputDataset, newList) as cursor:

            for row in cursor:
                if row[0] == None:
                    cursor.deleteRow()
                else:
                    row[1] = datetime.datetime.now().year - row[0].year
                    cursor.updateRow(row)
            del row
        del cursor
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "JoinAndCalc",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_age_field",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })
    finally:
        newList = None

        del newList

        gc.collect()


# ----------------------------------------------------------------------
def calculate_inline_stats(inputDataset, fields, result_field, stats_method):
    """calculate_inline_stats(inputDataset,(field1, field2,..),resultField,<Min, Max, Sum, Mean>)

    Calculates stats on the input table

     dataset(String):
    The specified feature class or table

     fields(field1,field2,..):
    List of fields to perform stats on

     result_field:
    Field to store the results on

     stats_method:
    Type of stats to perform
    """
    lstLen = None
    newList = None
    cnt = None
    val = None
    minVal = None
    maxVal = None
    try:

        lstLen = len(fields)
        newList = deepcopy(fields)
        newList.append(result_field)
        with arcpy.da.UpdateCursor(inputDataset, tuple(newList)) as cursor:

            for row in cursor:
                row.pop(lstLen)
                if stats_method.upper() == "AVERAGE" or stats_method.upper() == "AVG" or stats_method.upper() == "MEAN":
                    cnt = 0
                    val = 0
                    for i in row:
                        if i is not None:
                            cnt += 1
                            val += i
                    row.append(val / cnt)
                elif stats_method.upper() == "MIN" or stats_method.upper() == "MINIMUM":
                    minVal = min(i for i in row if i is not None)
                    row.append(minVal)
                elif stats_method.upper() == "MAX" or stats_method.upper() == "MAXIMUM":
                    maxVal = max(i for i in row if i is not None)
                    row.append(maxVal)
                cursor.updateRow(row)
            del row
            del cursor

    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "calculate_inline_stats",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })
    finally:
        lstLen = None
        newList = None
        cnt = None
        val = None
        minVal = None
        maxVal = None

        del lstLen
        del newList
        del cnt
        del val
        del minVal
        del maxVal

        gc.collect()


# ----------------------------------------------------------------------
def deleteFC(in_datasets):
    for in_data in in_datasets:
        try:
            if in_data is not None:
                if arcpy.Exists(dataset=in_data):
                    arcpy.Delete_management(in_data=in_data)

        except Exception:
            print "Unable to delete %s" % in_data


# ----------------------------------------------------------------------
# Function to merge run time reports into a temp feature class to export to CSV.
# This create a new FC with the first report, then just append fields for subsequent reports.
def mergeAllReports(reportLayer, report, config):
    fieldList = None
    fieldNames = None
    _tempWorkspace = None
    _mergedFeature = None
    _mergedFeaturePath = None
    spatRef = arcpy.Describe(reportLayer).spatialReference
    try:
        if not 'ReportMerge' in report:
            report['ReportMerge'] = 'TRUE'
            print "Report is missing the ReportMerge parameter:  type string, values, True or False"
        if report['ReportMerge'].upper() == "YES" or report['ReportMerge'].upper() == "TRUE":
            _tempWorkspace = config["ResultsGDB"]
            _mergedFeature = tempCSVName
            _mergedFeaturePath = os.path.join(_tempWorkspace, _mergedFeature)

            if arcpy.Exists(_mergedFeaturePath) == False:
                print "%s created" % _mergedFeaturePath
                arcpy.CreateFeatureclass_management(_tempWorkspace, _mergedFeature, "POLYGON", reportLayer, "DISABLED",
                                                    "DISABLED", spatRef, "", "0", "0", "0")
                arcpy.Append_management(reportLayer, _mergedFeaturePath, "TEST", "", "")
                # arcpy.DeleteField_management(_mergedFeaturePath, ["SHAPE_LENGTH", "SHAPE_AREA"])
            else:
                fieldNames = []
                fieldList = arcpy.ListFields(reportLayer)
                for field in fieldList:
                    # print field.name
                    if (field.name.upper() != "SHAPE") & (field.name.upper() != "SHAPE_LENGTH") & (
                        field.name.upper() != "SHAPE_AREA") & (field.name != report["ReportDateField"]) & (
                        field.name != report["ReportIDField"]):
                        fieldNames.append(field.name)

                fieldNames = ';'.join(fieldNames)

                arcpy.JoinField_management(_mergedFeaturePath, report["ReportIDField"], reportLayer,
                                           report["ReportIDField"], fieldNames)

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "mergeAllReports",
            "line": line,
            "filename": filename,
            "synerror": synerror,
            "arcpyError": arcpy.GetMessages(2),
        }
        )
    except:
        line, filename, synerror = trace()
        raise ReportToolsError({
            "function": "mergeAllReports",
            "line": line,
            "filename": filename,
            "synerror": synerror,
        })
    finally:

        del _tempWorkspace
        del _mergedFeature
        del _mergedFeaturePath
        del fieldList
        del fieldNames
        gc.collect()
