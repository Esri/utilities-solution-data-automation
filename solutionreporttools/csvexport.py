"""
    @author: ArcGIS for Gas Utilities
    @contact: ArcGISTeamUtilities@esri.com
    @company: Esri
    @version: 1.0
    @description: Class is used to export a feature into CSV using field alias
    @requirements: Python 2.7.x, ArcGIS 10.2
    @copyright: Esri, 2015
    @original source of script is from http://mappatondo.blogspot.com/2012/10/this-is-my-python-way-to-export-feature.html with modifications
"""
import sys, arcpy, csv
from arcpy import env
class ReportToolsError(Exception):
    """ raised when error occurs in utility module functions """
    pass
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
class CSVExport:
    _tempWorkspace = None
    _layers = None
    _CSVLocation = None

    def __init__(self, CSVLocation="", layer=None, workspace = None):
        # Gets the values of where the temp feature class resides and
        # the output location of the CSV.
        try:

            self._tempWorkspace = workspace
            self._layer = layer
            self._CSVLocation = CSVLocation

        except arcpy.ExecuteError:
            line, filename, synerror = trace()
            raise ReportToolsError({
                "function": "create_report_layers_using_config",
                "line": line,
                "filename":  filename,
                "synerror": synerror,
                "arcpyError": arcpy.GetMessages(2),
            }
            )
        except:
            line, filename, synerror = trace()
            raise ReportToolsError({
                "function": "create_report_layers_using_config",
                "line": line,
                "filename":  filename,
                "synerror": synerror,
            }
            )


    def WriteCSV(self):
        # This function writes the CSV. It writes the header then the rows. This script omits the SHAPE fields.
        try:
            env.workspace = self._tempWorkspace

            #fc = arcpy.ListFeatureClasses(self._layers)
            # for fcs in self._layer:
            fcs = self._layer
            if arcpy.Exists(fcs):
                with open(self._CSVLocation, 'wb') as outFile:
                    print "%s create" % self._CSVLocation
                    linewriter = csv.writer(outFile, delimiter = ',')

                    fcdescribe = arcpy.Describe(fcs)
                    flds = fcdescribe.Fields

                    # skip shape fields and derivatives
                    attrs = ("areaFieldName", "lengthFieldName", "shapeFieldName")
                    resFields = [getattr(fcdescribe, attr) for attr in attrs
                                    if hasattr(fcdescribe, attr)]

                    header,fldLst = zip(*((fld.AliasName, fld.name) for fld in flds
                                            if fld.name not in resFields))

                    linewriter.writerow([h.encode('utf8') if isinstance(h, unicode) else h for h in header])
                    linewriter.writerows([[r.encode('utf8') if isinstance(r, unicode) else r for r in row]
                                            for row in arcpy.da.SearchCursor(fcs, fldLst)])

                print "CSV file complete"
            return True
        except arcpy.ExecuteError:
            line, filename, synerror = trace()
            raise ReportToolsError({
                "function": "create_report_layers_using_config",
                "line": line,
                "filename":  filename,
                "synerror": synerror,
                "arcpyError": arcpy.GetMessages(2),
            }
            )
        except:
            line, filename, synerror = trace()
            raise ReportToolsError({
                "function": "create_report_layers_using_config",
                "line": line,
                "filename":  filename,
                "synerror": synerror,
            }
            )
