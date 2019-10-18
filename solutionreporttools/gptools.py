from __future__ import print_function
import os
import time
import datetime
import arcpy
import copy
from . import common as Common
from collections import defaultdict


def speedyIntersect(fcToSplit,
                    splitFC,
                    fieldsToAssign,
                    countField,
                    onlyKeepLargest,
                    outputFC,
                    report_areas_overlap):
    #arcpy.AddMessage(time.ctime())

    startProcessing = time.time()
    arcpy.env.overwriteOutput = True
    tempWorkspace = arcpy.env.scratchGDB
    tempFCName = Common.random_string_generator()
    tempFC= os.path.join(tempWorkspace, tempFCName)



    tempFCUnionName = Common.random_string_generator()
    tempFCUnion = os.path.join(tempWorkspace, tempFCUnionName)

    arcpy.Dissolve_management(in_features=splitFC,
                              out_feature_class=tempFCUnion,
                             dissolve_field=fieldsToAssign,
                             statistics_fields=None,
                             multi_part='SINGLE_PART',
                             unsplit_lines=None)


    fc = splitByLayer(fcToSplit=fcToSplit,
                      splitFC=tempFCUnion,
                      fieldsToAssign=fieldsToAssign,
                      countField=countField,
                      onlyKeepLargest=onlyKeepLargest,
                      outputFC=outputFC,
                      report_areas_overlap=report_areas_overlap)
    if arcpy.Exists(tempFCUnion):
        arcpy.Delete_management(tempFCUnion)
def assignFieldsByIntersect(sourceFC, assignFC, fieldsToAssign, outputFC,report_areas_overlap):
    tempWorkspace = arcpy.env.scratchGDB

    assignFields = arcpy.ListFields(dataset=assignFC)
    assignFieldsNames = [f.name for f in assignFields]

    sourceFields = arcpy.ListFields(dataset=sourceFC)
    sourceFieldNames = [f.name for f in sourceFields]
    newFields = []

    fms = arcpy.FieldMappings()
    for fieldToAssign in fieldsToAssign:
        if fieldToAssign not in assignFieldsNames:
            raise ValueError("{0} does not exist in {1}".format(fieldToAssign,assignFC))
        outputField = fieldToAssign
        if fieldToAssign in sourceFieldNames + newFields:
            outputField = Common.uniqueFieldName(fieldToAssign, sourceFieldNames + newFields)

        newFields.append(outputField)

        fm = arcpy.FieldMap()
        fm.addInputField(assignFC, fieldToAssign)
        type_name = fm.outputField
        type_name.name = outputField
        fm.outputField = type_name
        fms.addFieldMap(fm)



    fieldmappings = arcpy.FieldMappings()
    #fieldmappings.addTable(assignFC)
    #fieldmappings.removeAll()
    fieldmappings.addTable(sourceFC)
    for fm in fms.fieldMappings:
        fieldmappings.addFieldMap(fm)

    if report_areas_overlap:
        join_operation = "JOIN_ONE_TO_MANY"
    else:
        join_operation = "JOIN_ONE_TO_ONE"
    outputLayer = arcpy.SpatialJoin_analysis(target_features=sourceFC,
                                             join_features=assignFC,
                                             out_feature_class=outputFC,
                                             join_operation=join_operation,
                               join_type="KEEP_COMMON",
                              field_mapping=fieldmappings,
                              match_option="HAVE_THEIR_CENTER_IN",
                              search_radius=None,
                              distance_field_name=None)[0]


    return outputLayer
def splitByLayer(fcToSplit, splitFC, fieldsToAssign, countField, onlyKeepLargest, outputFC, report_areas_overlap):

    desc = arcpy.Describe(fcToSplit)
    path, fileName = os.path.split(outputFC)

    shapeLengthFieldName =""
    if desc.shapeType == "Polygon":
        shapeLengthFieldName = desc.areaFieldName
        dimension = 4
        measure = "area"
    elif desc.shapeType == "Polyline":
        shapeLengthFieldName = desc.lengthFieldName
        dimension = 2
        measure = "length"
    else:
        #arcpy.FeatureClassToFeatureClass_conversion(in_features=fcToSplit,
                                                    #out_path=path,
                                                    #out_name=fileName,
                                                    #where_clause=None,
                                                      #field_mapping=None,
                                                      #config_keyword=None)
        #TODO - verifiy this is the proper call on points
        assignFieldsByIntersect(sourceFC=fcToSplit,
                                assignFC=splitFC,
                                fieldsToAssign=fieldsToAssign,
                                outputFC=outputFC,
                                report_areas_overlap=report_areas_overlap)

        return outputFC

    arcpy.CreateFeatureclass_management(out_path=path,
                                        out_name=fileName,
                                        geometry_type=desc.shapeType,
                                        template=fcToSplit,
                                        has_m=None,
                                        has_z=None,
                                        spatial_reference=desc.spatialReference,
                                        config_keyword=None,
                                        spatial_grid_1=None,
                                        spatial_grid_2=None,
                                        spatial_grid_3=None)
    #Add the reporting name field to set in the split
    field_assign_object = arcpy.ListFields(dataset=splitFC,
                                           wild_card=fieldsToAssign[-1],
                                           field_type=None)
    #Find the freport label field and add it to the output line layer to store results in
    #field = [field for field in field_assign_object if field.name == fieldsToAssign[-1]][0]
    field = filter(lambda field:field.name == fieldsToAssign[-1],  field_assign_object)[0]
    arcpy.AddField_management(in_table=outputFC, field_name=field.baseName, field_type=field.type,
                                     field_precision=field.precision, field_scale=field.scale,
                                     field_length=field.length, field_alias=field.aliasName,
                                     field_is_nullable=field.isNullable, field_is_required=field.required,
                                     field_domain=field.domain)

    fldsInput1 = [f.name for f in arcpy.ListFields(fcToSplit) if f.name not in (desc.shapeFieldName,desc.oidFieldName,shapeLengthFieldName)] + \
        ["OID@","shape@"]
    fldsInsert = [arcpy.ValidateFieldName(f.name,path) for f in arcpy.ListFields(fcToSplit) if f.name not in (desc.shapeFieldName,desc.oidFieldName,shapeLengthFieldName)] + \
        [fieldsToAssign[-1],"OID@","shape@"]

    iOID = -2
    iShape = -1
    iAssignField = -3
    iCountField = None
    fndField = None
    if countField is not None and countField in fldsInput1:
        for f in arcpy.ListFields(outputFC):
            if f.name == countField:
                fndField = f
                break
        if fndField is None:
            raise ValueError("Count field not found")
        if fndField.type != "Double" and fndField.type != "Single" and fndField.type != "Integer" and fndField.type != "SmallInteger":
            raise ValueError("Count is not numeric")
        iCountField = fldsInput1.index(countField)

    with arcpy.da.SearchCursor(splitFC, ["Shape@","OID@",fieldsToAssign[-1]],spatial_reference=desc.spatialReference) as scursor:
        reportingGeometries = {row[1]:{"Geometry":row[0],fieldsToAssign[-1]:row[2]} for row in scursor}

    tempWorkspace = arcpy.env.scratchGDB
    tempFCName = Common.random_string_generator()
    tempFC= os.path.join(tempWorkspace, tempFCName)


    #Hide all fields to eliminate and Target_id, Join_FID conflicts
    target_fi = arcpy.FieldInfo()
    for field in desc.fields:
        target_fi.addField(field.name,field.name,'HIDDEN','NONE')

    source_fi = arcpy.FieldInfo()
    for field in arcpy.Describe(splitFC).fields:
        source_fi.addField(field.name,field.name,'HIDDEN','NONE')

    target_sj_no_fields = arcpy.MakeFeatureLayer_management(fcToSplit,"target_sj_no_fields",field_info=target_fi)
    join_sj_no_fields = arcpy.MakeFeatureLayer_management(splitFC,"join_sj_no_fields",field_info=source_fi)

    geoToLayerMap = arcpy.SpatialJoin_analysis(target_features=target_sj_no_fields,
                                               join_features=join_sj_no_fields,
                                               out_feature_class=tempFC,
                                               join_operation="JOIN_ONE_TO_MANY",
                                       join_type="KEEP_COMMON",
                                      field_mapping=None,
                                      match_option="INTERSECT",
                                      search_radius=None,
                                      distance_field_name=None)[0]

    ddict = defaultdict(list)

    with arcpy.da.SearchCursor(geoToLayerMap, ("TARGET_FID", "JOIN_FID")) as sCursor:
        for row in sCursor:
            ddict[row[0]].append(reportingGeometries[row[1]])

    layerToSplit = arcpy.MakeFeatureLayer_management(fcToSplit,"layerToSplit")
    result = arcpy.SelectLayerByLocation_management(layerToSplit, "CROSSED_BY_THE_OUTLINE_OF", splitFC)

    rowCount = int(arcpy.GetCount_management(layerToSplit)[0])
    j = 0
    rowsInserted = 0
    totalDif = 0
    with arcpy.da.SearchCursor(layerToSplit, fldsInput1) as scursor:
        with arcpy.da.InsertCursor(outputFC, fldsInsert) as icursor:

            for j,row in enumerate(scursor,1):
                newRows = []
                lens = []
                row = list(row)
                rowGeo = row[iShape]
                origLength =  getattr(rowGeo, measure)
                row[iShape] = None
                for geo in ddict[row[iOID]]:
                    newRow = copy.copy(row)
                    #if not row[iShape].disjoint(geo):
                    splitGeo = rowGeo.intersect(geo['Geometry'], dimension)

                    newRow[iShape] = splitGeo
                    splitLength = getattr(splitGeo, measure)
                    if iCountField is not None:
                        if row[iCountField] is not None and splitLength is not None and origLength is not None and origLength !=0:
                            newRow[iCountField] = float(row[iCountField]) * (float(splitLength) / float(origLength))
                        else:
                            pass
                    lens.append(float(splitLength))
                    #newRows.append(copy.copy(newRow))
                    newRow.insert(iAssignField + 1, geo[fieldsToAssign[-1]])
                    newRows.append(newRow)
                if onlyKeepLargest == True:
                    result = icursor.insertRow(newRows[lens.index(max(lens))])
                    rowsInserted = rowsInserted + 1
                else:
                    newOIDS = []
                    for newRow in newRows:
                        result = icursor.insertRow(newRow)
                        newOIDS.append(str(result))
                        rowsInserted = rowsInserted + 1
                    #if rowsInserted % 250 == 0:
                        #print (rowsInserted)
                    dif = sum(lens) / origLength
                    if (dif > 1.0001 or dif < .9999) and report_areas_overlap == False:
                        totalDif = totalDif + (origLength - sum(lens))
                        print ("Original Row ID: {3} and new features with OIDs of {0} combined count field did not add up to the original: new combined {1}, original {2}. \n This can be caused by self overlapping lines or data falling outside the split areas.  \n\tLayer: {4}".format(",".join(newOIDS),str(sum(lens)),str(origLength),row[iOID],desc.catalogPath))

    if totalDif > 0 and report_areas_overlap == False:
        print ("Total difference from source to results: {0}".format(totalDif))
    result = arcpy.SelectLayerByLocation_management(in_layer=layerToSplit,
                                                    selection_type="SWITCH_SELECTION")
    rowCount = int(arcpy.GetCount_management(layerToSplit)[0])
    if rowCount > 0:

        none_split_fc_name = Common.random_string_generator()
        none_split_fc= os.path.join(tempWorkspace, none_split_fc_name)

        assignFieldsByIntersect(sourceFC=layerToSplit,
                                    assignFC=splitFC,
                                    fieldsToAssign=fieldsToAssign,
                                    outputFC=none_split_fc,
                                    report_areas_overlap=report_areas_overlap)
        result = arcpy.Append_management(inputs=none_split_fc,
                                         target=outputFC,
                                         schema_type = "NO_TEST",
                                         field_mapping=None,
                                subtype=None)
        if arcpy.Exists(none_split_fc):
            arcpy.Delete_management(none_split_fc)
    if arcpy.Exists(tempFC):
        arcpy.Delete_management(tempFC)
    return outputFC

