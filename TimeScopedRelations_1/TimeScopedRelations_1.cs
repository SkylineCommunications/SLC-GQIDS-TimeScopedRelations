using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Filters;
using Skyline.DataMiner.Net.Helper;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.MetaData.DataClass;
using Skyline.DataMiner.Net;
using System;
using Skyline.DataMiner.Analytics.TimeScopedRelationDetection;
using Skyline.DataMiner.Analytics.Relations;

[GQIMetaData(Name = "TimeScopedRelations")]
public class TimeScopedRelations : IGQIDataSource, IGQIOnInit, IGQIOnPrepareFetch, IGQIInputArguments
{
    private static readonly GQIStringArgument ParameterA = new GQIStringArgument("Parameter") { IsRequired = false }; // make this not required to prevent errors in the dashboards where the value comes from another feed that could be empty
    private static readonly GQIDateTimeArgument StartTime = new GQIDateTimeArgument("Start Time") { IsRequired = false };// make this not required to prevent errors in the dashboards where the value comes from another feed that could be empty
    private static readonly GQIDateTimeArgument EndTime = new GQIDateTimeArgument("End Time") { IsRequired = false };// make this not required to prevent errors in the dashboards where the value comes from another feed that could be empty

    private static readonly GQIStringColumn ElementColumn = new GQIStringColumn("Related Element");
    private static readonly GQIStringColumn ParameterColumn = new GQIStringColumn("Related Parameter");
    private static readonly GQIStringColumn TableKeyColumn = new GQIStringColumn("Related TableKey");
    private static readonly GQIDoubleColumn TimeColumn = new GQIDoubleColumn("Confidence");

    private GQIDMS _dms;
    private ParamID _ParameterAValue;
    private DateTime? _StartTime;
    private DateTime? _EndTime;
    private DMSMessage[] _AnalyticsRelations;
    private Exception _LastError;

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    //For Building your query
    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[] { ParameterA, StartTime, EndTime };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        String paramKey;
        DateTime start;
        DateTime end;

        if (args.TryGetArgumentValue(ParameterA, out paramKey) && paramKey != null)
            _ParameterAValue = ParamID.FromString(paramKey);

        if (args.TryGetArgumentValue(StartTime, out start))
            _StartTime = start;

        if (args.TryGetArgumentValue(EndTime, out end))
            _EndTime = end;

        return new OnArgumentsProcessedOutputArgs();
    }

    public OnPrepareFetchOutputArgs OnPrepareFetch(OnPrepareFetchInputArgs args)
    {
        // Getting Time scoped relations            
        try
        {
            if (_ParameterAValue == null || _StartTime == null || _EndTime == null)
                return new OnPrepareFetchOutputArgs();

            _LastError = null;
            GetTimeScopedRelationsMessage req = new GetTimeScopedRelationsMessage(new Skyline.DataMiner.Analytics.DataTypes.ParameterKey(_ParameterAValue.DataMinerID, _ParameterAValue.EID, _ParameterAValue.PID, _ParameterAValue.TableIdx ?? ""), _StartTime.Value, _EndTime.Value);
            var resp = _dms.SendMessages(req) as DMSMessage[];

            if (resp != null)
            {
                _AnalyticsRelations = resp;
            }
        }
        catch (Exception ex)
        {
            _LastError = ex;
        }

        return new OnPrepareFetchOutputArgs();
    }

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
                ElementColumn,
                ParameterColumn,
                TableKeyColumn,
                TimeColumn
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        if (_LastError != null)
        {
            return new GQIPage(new GQIRow[1]{ new GQIRow(
                new[]
                {
                    new GQICell {Value= _LastError.ToString() }, // Related Element
                    new GQICell {Value= "" }, // Related Parameter,
                    new GQICell {Value= "" }, // Related TableKey,
                    new GQICell {Value= (double)0 } // Confidence,
                    
                }
                )
                    }
            );
        }

        var rows = new List<GQIRow>();
        if (_AnalyticsRelations != null)
        {
            foreach (DMSMessage ret in _AnalyticsRelations)
            {
                if (ret is GetTimeScopedRelationsResponseMessage)
                {
                    foreach (var relation in (ret as GetTimeScopedRelationsResponseMessage).AnalyticsRelations)
                    {
                        var cells = new[]{
                                new GQICell {Value= $"{relation.ParameterB.DataMinerID}/{relation.ParameterA.EID}"}, // Related Element
			                    new GQICell {Value= relation.ParameterB.GetKey() }, // Related Parameter,
			                    new GQICell {Value= relation.ParameterB.TableIdx }, // Related TableKey,
			                    new GQICell {Value= relation.Confidence }, // Confidence,		
			                };

                        var elementID = new ElementID(relation.ParameterB.DataMinerID, relation.ParameterA.EID);
                        var elementMetadata = new ObjectRefMetadata { Object = elementID };

                        var paramID = relation.ParameterB;
                        var paramMetadata = new ObjectRefMetadata { Object = paramID };

                        var rowMetadata = new GenIfRowMetadata(new RowMetadataBase[] { elementMetadata, paramMetadata });

                        rows.Add(new GQIRow(cells) { Metadata = rowMetadata });
                    }
                }
            }
        }
        return new GQIPage(rows.ToArray()) { HasNextPage = false };
    }
}