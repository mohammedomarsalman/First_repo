package com.informatica.adapter.onelake.runtime.adapter;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.infa.adapter.flatfileparser.flatfileparser.metadata.semantic.iface.SEMFlatFileParserDataformatConfigExtensions;
import com.informatica.adapter.fabric.onelake.OnelakeException;
import com.informatica.adapter.fabric.onelake.Operation;
import com.informatica.adapter.onelake.metadata.adapter.FieldInfo;
import com.informatica.adapter.onelake.metadata.adapter.OnelakeConnection;
import com.informatica.adapter.onelake.metadata.adapter.OnelakeConstants;
import com.informatica.adapter.onelake.onelake.metadata.semantic.iface.SEMOnelakeRecordExtensions;
import com.informatica.adapter.onelake.onelake.runtime.capability.semantic.auto.SAOnelakeReadCapabilityAttributesExtension;
import com.informatica.adapter.onelake.onelake.runtime.capability.semantic.auto.SAOnelakeWriteCapabilityAttributesExtension;
import com.informatica.adapter.onelake.runtimemessages.MessageBundle;
import com.informatica.adapter.sdkadapter.projection.projectionoperation.semantic.auto.SAD_ProjectionOperation;
import com.informatica.adapter.sdkadapter.projection.projectionoperation.semantic.manual.SD_ProjectionOperation;
import com.informatica.adapter.sdkadapter.projection.semantic.manual.SD_OperationBase;
import com.informatica.adapter.sdkadapter.projection.semantic.manual.SD_Projection;
import com.informatica.adapter.utils.runtime.impl.FileType;
import com.informatica.adapter.utils.runtime.impl.RuntimeUtilUserContext;
import com.informatica.sdk.adapter.javasdk.common.EIUDIndicator;
import com.informatica.sdk.adapter.javasdk.common.EIndicator;
import com.informatica.sdk.adapter.javasdk.common.ELogLevel;
import com.informatica.sdk.adapter.javasdk.common.EMessageLevel;
import com.informatica.sdk.adapter.javasdk.common.EReturnStatus;
import com.informatica.sdk.adapter.javasdk.common.Logger;
import com.informatica.sdk.adapter.javasdk.dataaccess.DataAttributes;
import com.informatica.sdk.adapter.javasdk.dataaccess.DataSession;
import com.informatica.sdk.adapter.javasdk.dataadapter.DataAdapter;
import com.informatica.sdk.adapter.javasdk.dataadapter.ReadAttributes;
import com.informatica.sdk.adapter.javasdk.dataadapter.WriteAttributes;
import com.informatica.sdk.adapter.javasdk.metadata.DataSourceOperationContext;
import com.informatica.sdk.adapter.javasdk.metadata.EmetadataHandleTypes;
import com.informatica.sdk.adapter.javasdk.metadata.RuntimeConfigMetadata;
import com.informatica.sdk.adapter.metadata.aso.semantic.iface.ASOOperation;
import com.informatica.sdk.adapter.metadata.capabilityattribute.semantic.iface.ASOComplexType;
import com.informatica.sdk.adapter.metadata.capabilityattribute.semantic.iface.CapabilityAttributes;
import com.informatica.sdk.adapter.metadata.capabilityattribute.semantic.iface.ReadCapabilityAttributes;
import com.informatica.sdk.adapter.metadata.capabilityattribute.semantic.iface.WriteCapabilityAttributes;
import com.informatica.sdk.adapter.metadata.extension.semantic.iface.Extension;
import com.informatica.sdk.adapter.metadata.patternblocks.dataformatconfig.semantic.iface.DataFormatConfig;
import com.informatica.sdk.adapter.metadata.patternblocks.flatrecord.semantic.iface.FlatRecord;
import com.informatica.sdk.adapter.metadata.patternblocks.shared.semantic.iface.ImportableObject;
import com.informatica.sdk.adapter.metadata.projection.helper.semantic.iface.BasicProjectionField;
import com.informatica.sdk.adapter.metadata.projection.helper.semantic.iface.BasicProjectionView;
import com.informatica.sdk.adapter.metadata.projection.mergeoperation.semantic.iface.MergeOperation;
import com.informatica.sdk.adapter.metadata.projection.projectionoperation.semantic.iface.ProjectionOperation;
import com.informatica.sdk.adapter.metadata.projection.semantic.iface.Projection;
import com.informatica.sdk.adapter.parser.metadata.consumerapi.ParserType;
import com.informatica.sdk.adapter.parser.runtime.consumerapi.DataParserFactory;
import com.informatica.sdk.adapter.parser.runtime.consumerapi.DataParserHelper;
import com.informatica.sdk.adapter.parser.runtime.iface.DataParser;
import com.informatica.sdk.adapter.parser.runtime.iface.InputData;
import com.informatica.sdk.adapter.parser.runtime.iface.ParserContext;
import com.informatica.sdk.adapter.parser.runtime.iface.Status;
import com.informatica.sdk.exceptions.SDKException;
@SuppressWarnings("unused")
public class OnelakeOnelakeDataAdapter extends DataAdapter  {

    private Logger logger = null;
    private RuntimeConfigMetadata runtimeMetadataHandle;
    private List<BasicProjectionField> projectionFields = null;
    private List<FieldInfo> connectedFields = null;
    private CapabilityAttributes capAttrs = null;
    private BasicProjectionView projectionView = null;
    private List<ImportableObject> nativeRecords = null;
    
    private String recordName;
    private String recordNativeName;
    private long dataPreviewRowCount;
    private OnelakeRead reader;
    private OnelakeWrite writer;
    private OnelakeConnection connection;
    private OnelakeOnelakeDataConnection conn;
    
    private FileType fileType = FileType.Flat;
    private FlatRecord sdkRecord;
    private DataParserFactory factory = null;
	private ParserContext parserContext = null;
	private DataParser dataParser;
	private DataSession dataSession;
	private boolean complexFileParser = false;
	private boolean flatFileParser=false;
	private boolean jsonFileParser=false;
	private boolean binaryFile=false;
	private boolean appendWriteStrategy=false;
	private String compressionFormatWrite="None";
	private boolean overridesHandledInMetadata = false;
	private boolean removeHeaders=false;
	
    /**
     * Initializes the data session. This method is called once for the current 
     * plugin -> Data Source Object -> Thread and is called sequentially for each thread.
     * 
     * @param dataSession
     *            The dataSession instance, which is the container for SDK handles.
     * @return EReturnStatus The status of the initialization call.
     */

    @Override
    public int initDataSession(DataSession dataSession) throws SDKException  {
    	// Use the logger for logging messages to the session log 
    	//as logMessage(ELogLevel.TRACE_NONE, Messages.CONN_SUCC_200, "user",6005,5.2);
    	this.dataSession = dataSession;
    	this.logger = dataSession.getInfaUtilsHandle().getLogger();

    	// The runtime metadata handle allows access to runtime metadata information
    	this.runtimeMetadataHandle = (RuntimeConfigMetadata) dataSession.getMetadataHandle(EmetadataHandleTypes.runtimeConfigMetadata);
    	// Use the basic projection view to get basic details like the projected fields/ filter / join metadata
    	this.projectionView = runtimeMetadataHandle.getBasicProjection();
    	// projectionFields has all fields of the native object to the platform source/target	
    	this.projectionFields = projectionView.getProjectionFields();
    	// connected fields is the subset of fields which are actually used in the mapping.
    	// use to fetch/write data to/from the native source
    	this.connectedFields= getConnectedFields(runtimeMetadataHandle);

    	// native flatrecord list used in the data session
    	this.nativeRecords = projectionView.getNativeRecords();

    	// handle to the list of capability attributes. Get the read/write capability details using this list
    	this.capAttrs = runtimeMetadataHandle.getCapabilityAttributes();

    	// connection object reference that can be used for data processing
    	this.conn = (OnelakeOnelakeDataConnection)dataSession.getConnection();
    	this.connection = conn.getMetadataConn();

    	this.sdkRecord = ( (FlatRecord) projectionView.getNativeRecords().get( 0 ) );
    	this.recordName = sdkRecord.getName();
    	this.recordNativeName = sdkRecord.getNativeName();
    	SEMOnelakeRecordExtensions extn = (SEMOnelakeRecordExtensions) sdkRecord.getExtensions();
		if (extn != null)
			this.overridesHandledInMetadata = extn.isOverridesHandledInMetadata();
    	
    	if ( capAttrs instanceof ReadCapabilityAttributes ) {
    		try{
				return this.initReadRuntime();
    		}catch ( Exception e ) {
                logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, "Exception: "+
                        e.getMessage() );
                    return EReturnStatus.FAILURE;
    		}
    	}
    	else if ( capAttrs instanceof WriteCapabilityAttributes ) {
            try {
            	return this.initWriteRuntime();
              } catch ( Exception e ) {
                logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, "Exception: " + e.getMessage());
                return EReturnStatus.FAILURE;
            }
        } else {
            logger
                .logMessage( EMessageLevel.MSG_FATAL_ERROR, ELogLevel.TRACE_NORMAL, "Unsupported Operation type" );
            return EReturnStatus.FAILURE;
        }
    }


	/**
     * Gets the list of connected fields in the read or write operation. If the method is 
     * referenced with any unconnected fields, the method may encounter errors.
     *
     * @param runtimeMetadataHandle 
     *            The run-time metadata handle of the DataSession.
     * @return A list of Fields that are connected in the read or write operation.
     */

    private ArrayList<FieldInfo> getConnectedFields(RuntimeConfigMetadata runtimeMetadataHandle){
    	int i = 0;
    	ArrayList<FieldInfo> fields = new ArrayList<FieldInfo>();
    	for (BasicProjectionField pfield : projectionFields) {
    		if (runtimeMetadataHandle.isFieldConnected(i)) { 
    			FieldInfo f = new FieldInfo(pfield,i); 
    			fields.add(f);
    		}
    		i++;
    	}
    	return fields;
    }

    /**
     * Begins the data session once for the current plugin -> DSO -> thread 
     * which could be called in parallel for each thread. If connection pooling 
     * is enabled, then the same connection can initialize multiple data sessions.
     * 
     * @param dataSession
     *            The Data session instance, which is the container for SDK handles.
     * @return EReturnStatus The status of the begin session call.
     */

    @Override
    public int beginDataSession(DataSession dataSession){
    	return EReturnStatus.SUCCESS;
    }



    /**
     * Ends the data session once for the current plugin -> DSO -> thread which 
     * could be called in parallel for each thread. If connection pooling is enabled, 
     * then the same connection can initialize multiple data sessions.
     * 
     * @param dataSession
     *            The Data session instance, which is the container for SDK handles.
     * @return EReturnStatus The status of the end session call.
     */

    @Override
    public int endDataSession(DataSession dataSession){
    	return EReturnStatus.SUCCESS;
    }



    /**
     * Deinitializes the data session. This method is called once for the current 
     * plugin -> Data Source Object -> Thread and is called sequentially for each thread.
     * 
     * @param dataSession
     *            The dataSession instance, which is the container for SDK handles.
     * @return EReturnStatus The status of the deinitialization call.
     */

    @Override
    public int deinitDataSession(DataSession dataSession){
    	if ( reader != null) {
    		try {
                reader.deinit();
            } catch ( Exception e ) {
                logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE,
                    e.getMessage() );
                return EReturnStatus.FAILURE;
            }
    	}
    	
    	try {
			if ( !complexFileParser && capAttrs instanceof WriteCapabilityAttributes)
				dataParser.deinit();				
		} catch (SDKException e) {
			logger.logMessage(EMessageLevel.MSG_ERROR,	ELogLevel.TRACE_NONE, e.getLocalizedMessage());
			throw new RuntimeException(e.getLocalizedMessage());					
		}
        if ( writer != null ) {
            try {
            	//For create target case, if the FileName port is coming from the source then parser deletes the original
				//staging file and creates a new one.
				if(flatFileParser && !writer.getStagingFile().exists()){
					ArrayList<File> writerStagingFileList = (ArrayList<File>) parserContext.getParserMetadata();
					if(writerStagingFileList != null && !writerStagingFileList.isEmpty())
						writer.setStagingFile(Operation.INSERT, writerStagingFileList.get(0));
				}
            	//Compress the staging file passed
            	if(flatFileParser&&compressionFormatWrite.equalsIgnoreCase("Gzip"))
            		writer.setCompressedStagingFile(); 
            	
            	if(flatFileParser&&appendWriteStrategy)
            		writer.setRemoveHeader(removeHeaders);
					writer.deinit();		
            }
             catch ( Exception e) {
        	  logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE,
                      e.getMessage());
              return EReturnStatus.FAILURE;
            }
        }
    	return EReturnStatus.SUCCESS;
    }



    /**
     * Reads data from the external data source. Returning NO_MORE_DATA after 
     * completion of reading data, stops calling the read method again. Returning 
     * a success informs SDK to continue calling the method.
     * 
     * @param dataSession
     *            The dataSession instance, which is the container for SDK handles.
     * @param readAttr
     *            The ReadAttributes that provides access to the read specific 
     *			  attributes for the data adapter passed during the read phase.
     * @return EReturnStatus The status of the read call.
     */

    @Override
    public int read(DataSession dataSession, ReadAttributes readAttr) throws SDKException  {
    	try {
    		if(complexFileParser)
        		return reader.complexFileRead(dataSession, readAttr);
        	
    		Map<Integer,Object> data = new HashMap<>();
    		InputData inputData = DataParserHelper.createInputData(data);
    		
    		Status status = dataParser.parse( inputData, dataSession, readAttr );
    		
    		if ( status == Status.Failure ) {
    			logger.logMessage( EMessageLevel.MSG_FATAL_ERROR, ELogLevel.TRACE_NORMAL,
    					"Exception occured while reading data");
    			return EReturnStatus.FAILURE;
    		}
           return EReturnStatus.NO_MORE_DATA;
    	} catch (Exception e) {
    		logger.logMessage(EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, e.getMessage());
			return EReturnStatus.FAILURE;
    	}
    }


    /**
     * Sets the multiple row data in the data table to the data session buffer
     * <pre>
     * ##################################
     * AUTOGENERATED CODE
     * ##################################
     * </pre>
     *
     * @param dataSession
     *            The dataSession instance, which is the container for SDK handles.
     * @param dataTable
     *             List of List of Objects. Each List of Objects represents a single row.
     */

    private void setDataToPlatform(DataSession dataSession, List<List<Object>> dataTable) throws SDKException  {
         for (int row = 0; row < dataTable.size(); row++) {
    			List<Object> rowData = dataTable.get(row);
    			for (int col = 0; col < connectedFields.size(); col++) {
    				DataAttributes pDataAttributes = new DataAttributes();
    				pDataAttributes.setDataSetId(0);
    				pDataAttributes.setColumnIndex(connectedFields.get(col).index);
    				pDataAttributes.setRowIndex(row);
    				Object data = rowData.get(col);

    				String dataType = connectedFields.get(col).field.getDataType();
    				String columnName = connectedFields.get(col).field.getName();

    				if (dataType.equalsIgnoreCase("string")
    						|| dataType.equalsIgnoreCase("text")) {
    					if (data == null) {
    						pDataAttributes.setLength(0);
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						String text = data.toString();

    						int fieldLength = connectedFields.get(col).field
    								.getPrec();
    						if (text.length() > fieldLength) {
    							pDataAttributes.setLength(fieldLength);
    							pDataAttributes.setIndicator(EIndicator.TRUNCATED);
    							data = text.substring(0, fieldLength);
    						} else {
    							pDataAttributes.setLength(text.length());
    							pDataAttributes.setIndicator(EIndicator.VALID);
    						}
    					}
    					dataSession.setStringData((String) data, pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("double") == 0) {
    					if (data instanceof Double) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					} else if (data instanceof Number) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    						data = ((Number) data).doubleValue();
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_ERROR,
    								ELogLevel.TRACE_NONE, "Data for column ["
    										+ columnName + "] of type [" + dataType
    										+ "] " + "should be a of type ["
    										+ Number.class.getName()
    										+ "] or its sub-types.");
    						data = null;
    					}
    					dataSession.setDoubleData((Double) data, pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("float") == 0) {
    					if (data instanceof Float) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					} else if (data instanceof Number) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    						data = ((Number) data).floatValue();
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_ERROR,
    								ELogLevel.TRACE_NONE, "Data for column ["
    										+ columnName + "] of type [" + dataType
    										+ "] " + "should be a of type ["
    										+ Number.class.getName()
    										+ "] or its sub-types.");
    						data = null;
    					}
    					dataSession.setFloatData((Float) data, pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("long") == 0) {
    					if (data instanceof Long) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					} else if (data instanceof Number) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    						data = ((Number) data).longValue();
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_ERROR,
    								ELogLevel.TRACE_NONE, "Data for column ["
    										+ columnName + "] of type [" + dataType
    										+ "] " + "should be a of type ["
    										+ Number.class.getName()
    										+ "] or its sub-types.");
    						data = null;
    					}
    					dataSession.setLongData((Long) data, pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("short") == 0) {
    					if (data instanceof Short)
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					else if (data instanceof Number) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    						data = ((Number) data).shortValue();
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_ERROR,
    								ELogLevel.TRACE_NONE, "Data for column ["
    										+ columnName + "] of type [" + dataType
    										+ "] " + "should be a of type ["
    										+ Number.class.getName()
    										+ "] or its sub-types.");
    						data = null;
    					}
    					dataSession.setShortData((Short) data, pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("integer") == 0) {
    					if (data instanceof Integer) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					} else if (data instanceof Number) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    						data = ((Number) data).intValue();
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_ERROR,
    								ELogLevel.TRACE_NONE, "Data for column ["
    										+ columnName + "] of type [" + dataType
    										+ "] " + "should be a of type ["
    										+ Number.class.getName()
    										+ "] or its sub-types.");
    						data = null;
    					}
    					dataSession.setIntData((Integer) data, pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("bigint") == 0) {
    					if (data instanceof Long) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					} else if (data instanceof Number) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    						data = ((Number) data).longValue();
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_ERROR,
    								ELogLevel.TRACE_NONE, "Data for column ["
    										+ columnName + "] of type [" + dataType
    										+ "] " + "should be a of type ["
    										+ Number.class.getName()
    										+ "] or its sub-types.");
    						data = null;
    					}
    					dataSession.setLongData((Long) data, pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("date/time") == 0) {
    					if (data instanceof Timestamp) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_ERROR,
    								ELogLevel.TRACE_NONE, "Data for column ["
    										+ columnName + "] of type [" + dataType
    										+ "]" + " should be a of type ["
    										+ Timestamp.class.getName() + "].");
    						data = null;
    					}
    					dataSession.setDateTimeData((Timestamp) data,
    							pDataAttributes);
    				} else if (dataType.compareToIgnoreCase("binary") == 0) {

    					if (data == null) {
    						pDataAttributes.setLength(0);
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else if (data instanceof byte[]) {
    						byte[] binData = (byte[]) data;
    						int fieldLength = connectedFields.get(col).field
    								.getPrec();

    						if (binData.length > fieldLength) {
    							pDataAttributes.setLength(fieldLength);
    							pDataAttributes.setIndicator(EIndicator.TRUNCATED);
    							data = Arrays.copyOf(binData, fieldLength);
    						} else {
    							pDataAttributes.setLength(binData.length);
    							pDataAttributes.setIndicator(EIndicator.VALID);

    						}
    					} else {
    						logger.logMessage(EMessageLevel.MSG_DEBUG,
    								ELogLevel.TRACE_VERBOSE_DATA, "Data for type ["
    										+ dataType + "] should be a of type ["
    										+ byte[].class.getName() + "].");
    						data = null;
    						pDataAttributes.setLength(0);
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					}
    					dataSession.setBinaryData((byte[]) data, pDataAttributes);    					
    				} else if (dataType.compareToIgnoreCase("decimal") == 0) {
    					if (data instanceof BigDecimal) {
    						pDataAttributes.setIndicator(EIndicator.VALID);
    					} else if (data == null) {
    						pDataAttributes.setIndicator(EIndicator.NULL);
    					} else {
    						logger.logMessage(EMessageLevel.MSG_DEBUG,
    								ELogLevel.TRACE_VERBOSE_DATA, "Data for type ["
    										+ dataType + "] should be a of type ["
    										+ BigDecimal.class.getName() + "].");
    						data = null;
    					}
    					dataSession.setBigDecimalData((BigDecimal) data,
    							pDataAttributes);
    				}
    			}
    		}
         
    }



    /**
     * Writes data to the external data source. The SDK continues to call this 
     * method until it completes writing data to the data source.
     * 
     * @param dataSession
     *            The dataSession instance, which is the container for SDK handles.
     * @param writeAttr
     *            The WriteAttributes that provides access to the write specific 
     *			  attributes for the data adapter passed during the read phase.
     * @return EReturnStatus The status of the write call.
     */

    @Override
    public int write(DataSession dataSession, WriteAttributes writeAttr) throws SDKException  {
    	
    	if(this.runtimeMetadataHandle.getRowIUDIndicator(0) != EIUDIndicator.INSERT) {
			switch(this.runtimeMetadataHandle.getRowIUDIndicator(0)) {
			case 1:
				logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NORMAL,
			            " Error occured while writing data to the target native system : [Operation type 'Update' not supported.] " );
				return EReturnStatus.FAILURE;
			case 2:
				logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NORMAL,
			            "Error occured while writing data to the target native system : [Operation type 'Delete' not supported.] " );
				return EReturnStatus.FAILURE;
			}
		}
    	
    	if(complexFileParser)
    		return writer.complexFileWrite(dataSession, writeAttr);
    	OnelakeOutputData outputData = new OnelakeOutputData();
    	
    	if(this.fileType.equals(FileType.Json))
        	outputData.setData(writer.getStagingFile());
    	Status status = dataParser.build( outputData, dataSession, writeAttr );
        if ( status == Status.Failure ) {
            logger.logMessage( EMessageLevel.MSG_FATAL_ERROR, ELogLevel.TRACE_NORMAL,
            		"File parsing failed, exiting read runtime execution with error." );
            return EReturnStatus.FAILURE;
        }
        
        return EReturnStatus.SUCCESS;
    }
    
	
    /**
     * This API should be implemented by adapter developer in conjunction with read
     * API to implement lookup. SDK will provide updated filter condition with reset API.
     * Adapter developer are expected to reset the adapter context in reset API. 
     * 
     * @param dataSession
     *            DataSession instance
     * @return EReturnStatus
     */

    @Override
    public int reset(DataSession dataSession) throws SDKException  {
    	return EReturnStatus.SUCCESS;
    }



    /**
     * Log a localized message to session log.
     * 
     * @param logLevel
     *		ELogLevel Trace level at which the message should be logged.
     * @param messageKey
     *            Message Key of the Message.
     * @param messageFormatArguments
     *			  Message Format arguments.
     * @return EReturnStatus The status of the logger call.
     */

    private int logMessage(int logLevel, String messageKey, Object... messageFormatArguments){
    	if (this.logger != null) {
    		return logger.logMessage(MessageBundle.getInstance(),logLevel, messageKey, messageFormatArguments);
    	}
    	return EReturnStatus.FAILURE;
    }

    
    public boolean checkDataPreview(RuntimeConfigMetadata runtimeMetadataHandle){
    	
		ASOOperation m_asoOperation = runtimeMetadataHandle
				.getAdapterDataSourceOperation();
		ASOComplexType asoComplexType = m_asoOperation.getComplexTypes().get(0);
		List<Projection> projectionList = asoComplexType.getProjections();
		
		for (int j = 0; j < projectionList.size(); j++) {
			Projection proj = projectionList.get(j);
			if (proj == null) {
				logger.logMessage(MessageBundle.getInstance(),
						ELogLevel.TRACE_NONE, "Projection list is null");
				return false;
			}
			SD_Projection sd_projection = (SD_Projection)proj;
			List<SD_OperationBase> sd_OperationBaseList = sd_projection.getAllOfType(SAD_ProjectionOperation.class);
			//List<OperationBase> baseOperList = proj.getBaseOperations();
			if (sd_OperationBaseList == null) {
				logger.logMessage(MessageBundle.getInstance(),
						ELogLevel.TRACE_NONE, "Base Operation list is null");
				return false;
			}
			
			for (int itrProj = 0; itrProj < sd_OperationBaseList.size(); itrProj++) {
				SD_OperationBase ob = sd_OperationBaseList.get(itrProj);
				if (ob == null) {
					logger.logMessage(MessageBundle.getInstance(),
							ELogLevel.TRACE_NONE,
							"Operation base is null");
					return false;
				}
				
				if (ob instanceof ProjectionOperation){
					SD_ProjectionOperation projectionOperation = (SD_ProjectionOperation) ob;
					dataPreviewRowCount = projectionOperation.getMaxRows();
				}
			}
		}
		if(dataPreviewRowCount > 0)
			return true;
		else 
			return false;
	}
    
    public int initReadRuntime() throws Exception{
    	ReadCapabilityAttributes attrs = (ReadCapabilityAttributes)capAttrs;
		Extension extn = attrs.getExtensions();
		
		if ( extn instanceof SAOnelakeReadCapabilityAttributesExtension ) {
			SAOnelakeReadCapabilityAttributesExtension attribs = (SAOnelakeReadCapabilityAttributesExtension) extn;
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Workspace Override: " + attribs.getWorkspaceOverride());
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Lakehouse Override: " + attribs.getLakehouseOverride());
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "File name Override: " + attribs.getFileNameOverride());
			//Log Display Name
			String sourceType =  attribs.getSourceType().contains("RecursiveDirectory") ? "Directory - Recursive" : attribs.getSourceType();
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Source Type: " + sourceType);
			
		} else {
			throw new OnelakeException( "Unknown read capability attributes" );
		}
		DataParserFactory factory = new DataParserFactory();
		parserContext = DataParserHelper
                .createParserContext( dataSession , new HashMap<Integer,Object>(), new ArrayList<Object> ());
		try {
			reader = new OnelakeRead( dataSession, (ReadCapabilityAttributes) capAttrs, connection, recordName, recordNativeName,
	    			connectedFields, dataPreviewRowCount, logger, overridesHandledInMetadata );
		} catch (RuntimeException e) {
			logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, e.getMessage());
   		    return EReturnStatus.FAILURE;
		}
           
        MergeOperation deserializationSpec =  parserContext.getDeserializationSpecs();
        
		if(deserializationSpec != null){
			DataFormatConfig dataFormatConfig = deserializationSpec.getAppliedDataFormatConfig(0);
			fileType = FileType.valueOf(dataFormatConfig.getDataFormat());
		}
		else{
			fileType = FileType.Binary;
		}
		flatFileParser=fileType.equals(FileType.Flat);
		jsonFileParser=fileType.equals(FileType.Json);
		
		if(fileType.equals(FileType.Flat)  || fileType.equals(FileType.Binary) || fileType.equals(FileType.Json))
    	{
    			reader.init();
        
                logger.logMessage( EMessageLevel.MSG_TRACE, ELogLevel.TRACE_VERBOSE_DATA, "Initializing the parser" );

                Map<Integer, Object> map = new HashMap<>();
                List<Object> parserMetadata = new ArrayList<Object> ();
                Map<File, Boolean> stagingFilesMap = reader.getStagingFilesMap();	
                int fileIndex=0;
            	for(Entry<File, Boolean> fileEntry : stagingFilesMap.entrySet())
        		{
        			if(fileEntry.getValue())//Compression is true
        			{     	
        				extn = ((ReadCapabilityAttributes)capAttrs).getExtensions();
        				String compressionFormat = null;       				
        				if ( extn instanceof SAOnelakeReadCapabilityAttributesExtension ) {
        					SAOnelakeReadCapabilityAttributesExtension attribs = (SAOnelakeReadCapabilityAttributesExtension) extn;
        					compressionFormat =  attribs.getCompressionFormat();
        					if(!compressionFormat.equalsIgnoreCase("None") && fileType.equals(FileType.Json)) {
        						String errMsg ="OneLake does not support reading compressed JSON files";
    		                	logger.logMessage(EMessageLevel.MSG_ERROR,	ELogLevel.TRACE_NONE,errMsg);
                				throw new RuntimeException(errMsg);
			                }
        				if(!(compressionFormat.equalsIgnoreCase("Gzip")||compressionFormat.equalsIgnoreCase("None")))
        				{
        					logger.logMessage(EMessageLevel.MSG_ERROR,	ELogLevel.TRACE_NONE,"Compression format " +compressionFormat+ " is not valid. One Lake adapter supports only Gzip compression format for " + fileType.toString() + " resource in native mode of execution.");
        				throw new RuntimeException("Compression format " +compressionFormat+ " is not valid. Onelake supports only Gzip compression format for " + fileType.toString() + " resource in native environment.");
        				}
        			}
        			}
        		   parserMetadata.add(fileEntry.getKey());
        		   fileIndex++;
        		}
            	if(!fileType.equals(FileType.Json))
            		map.put( 0, parserMetadata );
            	parserContext = DataParserHelper.createParserContext( dataSession, map, parserMetadata );
            	
            	if(fileType.equals(fileType.Json))
            		dataParser = factory.getDataParser(ParserType.json);
            	else
            		dataParser = factory.getDataParser(ParserType.Flat);
            	dataParser.init( parserContext );
            	
    	}else if(fileType.equals(FileType.Parquet) || fileType.equals(FileType.Avro) || fileType.equals(FileType.Orc)){
    		complexFileParser = true;
    	}else{
    		logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, "File Type: "+fileType.toString()+" is not supported");
    		 return EReturnStatus.FAILURE;
    	}
		if( complexFileParser )
		{
			DataSourceOperationContext dsoHandle = (DataSourceOperationContext) dataSession
					.getMetadataHandle(EmetadataHandleTypes.datasourceoperationMetadata);
			RuntimeUtilUserContext userContext = null;
			if (dsoHandle != null) {
				userContext = (RuntimeUtilUserContext) dsoHandle.getUserHandle();
			}
			reader.complexFileInit(fileType, userContext);}
		if(fileType.equals(FileType.Binary))
			binaryFile=true;
		else
			binaryFile=false;		
		
    	return EReturnStatus.SUCCESS;
    }
    
    public int initWriteRuntime() throws Exception{
    	String writeStrategy = OnelakeConstants.WRITE_STRATEGY_OVERWRITE;
    	WriteCapabilityAttributes attrs = (WriteCapabilityAttributes)capAttrs;
		Extension extn = attrs.getExtensions();
		if ( extn instanceof SAOnelakeWriteCapabilityAttributesExtension ) {
			SAOnelakeWriteCapabilityAttributesExtension attribs = (SAOnelakeWriteCapabilityAttributesExtension) extn;
			writeStrategy = attribs.getWriteStrategy();
			if(writeStrategy.equalsIgnoreCase(OnelakeConstants.WRITE_STRATEGY_APPEND))
				appendWriteStrategy=true;
			else
				appendWriteStrategy=false;
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Workspace Override: " + attribs.getWorkspaceOverride());
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Lakehouse Override: " + attribs.getLakehouseOverride());
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "File name Override: " + attribs.getFileNameOverride());
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Write Strategy: " + attribs.getWriteStrategy());
			logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Compression Format: " + attribs.getCompressionFormat());
		} else {
			throw new OnelakeException( "Unknown write capability attributes" );
		}
		DataParserFactory factory = new DataParserFactory();
    	 parserContext = DataParserHelper
                .createParserContext( dataSession , new HashMap<Integer,Object>(), new ArrayList<Object> ());
    	try {
    		writer = new OnelakeWrite(dataSession, (WriteCapabilityAttributes) capAttrs, connection,
                    runtimeMetadataHandle, recordName, recordNativeName, connectedFields, logger, overridesHandledInMetadata);
    	} catch (RuntimeException e) {
    		logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, e.getMessage());
   		    return EReturnStatus.FAILURE;
    	}
    	
            
        MergeOperation deserializationSpec =  parserContext.getDeserializationSpecs();
		if(deserializationSpec != null){
			DataFormatConfig dataFormatConfig = deserializationSpec.getAppliedDataFormatConfig(0);
			fileType = FileType.valueOf(dataFormatConfig.getDataFormat());
		}
		else{
			fileType = FileType.Binary;
		}
		
		flatFileParser = fileType.equals(FileType.Flat);
		
		jsonFileParser=fileType.equals(FileType.Json);
	
		binaryFile=fileType.equals(FileType.Binary);
		
		if(flatFileParser && appendWriteStrategy ) {
			// This block is used only for FF parser, For AppendWriteStrategy, the headers should be removed only if Target is with header
			 SEMFlatFileParserDataformatConfigExtensions extnFF = (SEMFlatFileParserDataformatConfigExtensions) deserializationSpec.getAppliedDataFormatConfig(0).getExtensions();
			 if(extnFF!=null) {
				 String TargetHeader = extnFF.getTargetHeader();
				 if(TargetHeader.equalsIgnoreCase("With Header")) {
					 this.removeHeaders = true;
					 writer.setRowDilimiter(extnFF.getRowDelimiter());
				 }
			 }
		}
				
        if(fileType.equals(FileType.Flat)  || fileType.equals(FileType.Binary) || fileType.equals(FileType.Json)){
        	writer.init();
        	File stagingFile = writer.getStagingFile();
      
            boolean isCompressedOutputStream = writer.isCompressedStream();
            logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, "Initializing the parser" );

            Map<Integer, Object> map = new HashMap<>();
            List<Object> parserMetadata = new ArrayList<Object> ();
            if ( extn instanceof SAOnelakeWriteCapabilityAttributesExtension ) {
    			SAOnelakeWriteCapabilityAttributesExtension attribs = (SAOnelakeWriteCapabilityAttributesExtension) extn;
    			compressionFormatWrite = attribs.getCompressionFormat();
    			if(!compressionFormatWrite.equalsIgnoreCase("None") && fileType.equals(FileType.Json)) {
					String errMsg ="OneLake does not support writing compressed JSON files";
                	logger.logMessage(EMessageLevel.MSG_ERROR,	ELogLevel.TRACE_NONE,errMsg);
    				throw new RuntimeException(errMsg);
                }
        	}
            if(isCompressedOutputStream)
            {			
    				if(!(compressionFormatWrite.equalsIgnoreCase("Gzip")||compressionFormatWrite.equalsIgnoreCase("None")))
    				{
    					logger.logMessage(EMessageLevel.MSG_ERROR,	ELogLevel.TRACE_NONE,"Compression format " +compressionFormatWrite+ " is not valid. One Lake adapter supports only Gzip compression format for " + fileType.toString() + " resource in native mode of execution.");
    				throw new RuntimeException("Compression format " +compressionFormatWrite+ " is not valid. Onelake supports only Gzip compression format for " + fileType.toString() + " resource in native environment.");
    				}
            }  
            parserMetadata.add(stagingFile);                 
            //map.put( 0, parserMetadata );  
            
            parserContext = DataParserHelper
                    .createParserContext( dataSession, map, parserMetadata );
          
            if(fileType.equals(fileType.Json))
        		dataParser = factory.getDataParser(ParserType.json);
        	else
        		dataParser = factory.getDataParser(ParserType.Flat);
            dataParser.init( parserContext );
            
        }else if(fileType.equals(FileType.Parquet) || fileType.equals(FileType.Avro) || fileType.equals(FileType.Orc)){
    		complexFileParser = true;
    	}else{
    		logger.logMessage( EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, "File Type: "+fileType.toString()+" is not supported");
    		 return EReturnStatus.FAILURE;
    	}
		if( complexFileParser )
		{
			writer.complexFileInit(fileType);
		}
    	return EReturnStatus.SUCCESS;
    }

}
