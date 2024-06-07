using System;
using System.Collections.Generic;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Analytics.TimeScopedRelationDetection;
using Skyline.DataMiner.Net;
using Skyline.DataMiner.Net.Messages;

[GQIMetaData(Name = "TimeScopedRelations")]
public class TimeScopedRelations : IGQIDataSource, IGQIOnInit, IGQIOnPrepareFetch, IGQIInputArguments
{
    private static readonly GQIStringArgument ParameterA = new GQIStringArgument("Parameter") { IsRequired = false }; // make this not required to prevent errors in the dashboards where the value comes from another feed that could be empty
    private static readonly GQIDateTimeArgument StartTime = new GQIDateTimeArgument("Start Time") { IsRequired = false }; // make this not required to prevent errors in the dashboards where the value comes from another feed that could be empty
    private static readonly GQIDateTimeArgument EndTime = new GQIDateTimeArgument("End Time") { IsRequired = false }; // make this not required to prevent errors in the dashboards where the value comes from another feed that could be empty

    private static readonly GQIStringColumn ElementColumn = new GQIStringColumn("Related Element");
    private static readonly GQIStringColumn ParameterColumn = new GQIStringColumn("Related Parameter");
    private static readonly GQIStringColumn TableKeyColumn = new GQIStringColumn("Related TableKey");
    private static readonly GQIDoubleColumn TimeColumn = new GQIDoubleColumn("Confidence");

    private GQIDMS _dms;
    private ParamID _parameterAValue;
    private DateTime? _startTime;
    private DateTime? _endTime;
    private DMSMessage[] _analyticsRelations;
    private Exception _lastError;

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    public GQIArgument[] GetInputArguments()
    {
        return new GQIArgument[] { ParameterA, StartTime, EndTime };
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        if (args.TryGetArgumentValue(ParameterA, out string paramKey) && paramKey != null)
            _parameterAValue = ParamID.FromString(paramKey);

        if (args.TryGetArgumentValue(StartTime, out DateTime start))
            _startTime = start;

        if (args.TryGetArgumentValue(EndTime, out DateTime end))
            _endTime = end;

        return new OnArgumentsProcessedOutputArgs();
    }

    public OnPrepareFetchOutputArgs OnPrepareFetch(OnPrepareFetchInputArgs args)
    {
        // Getting Time scoped relations
        try
        {
            if (_parameterAValue == null || _startTime == null || _endTime == null)
                return new OnPrepareFetchOutputArgs();

            _lastError = null;
            GetTimeScopedRelationsMessage req = new GetTimeScopedRelationsMessage(new Skyline.DataMiner.Analytics.DataTypes.ParameterKey(_parameterAValue.DataMinerID, _parameterAValue.EID, _parameterAValue.PID, _parameterAValue.TableIdx ?? string.Empty), _startTime.Value, _endTime.Value);
            DMSMessage[] resp = _dms.SendMessages(req) as DMSMessage[];

            if (resp != null)
            {
                _analyticsRelations = resp;
            }
        }
        catch (Exception ex)
        {
            _lastError = ex;
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
                TimeColumn,
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        if (_lastError != null)
        {
            return new GQIPage(new GQIRow[1]
			{
				new GQIRow(
                new[]
                {
                    new GQICell {Value= _lastError.ToString() }, // Related Element
                    new GQICell {Value= string.Empty }, // Related Parameter,
                    new GQICell {Value= string.Empty }, // Related TableKey,
                    new GQICell {Value= 0D }, // Confidence,
                }),
			});
        }

        var rows = new List<GQIRow>();
        if (_analyticsRelations != null)
        {
            foreach (DMSMessage ret in _analyticsRelations)
            {
                if (ret is GetTimeScopedRelationsResponseMessage)
                {
                    foreach (var relation in (ret as GetTimeScopedRelationsResponseMessage).AnalyticsRelations)
                    {
                        var cells = new[]
						{
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