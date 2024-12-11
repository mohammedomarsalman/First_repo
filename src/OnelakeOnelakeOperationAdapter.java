package com.informatica.adapter.onelake.runtime.adapter;

import com.informatica.sdk.adapter.javasdk.dataadapter.DataSourceOperationAdapter;
import java.util.Map;
import java.util.HashMap;
import com.informatica.sdk.adapter.javasdk.common.ELogLevel;
import com.informatica.sdk.adapter.javasdk.common.EMessageLevel;
import com.informatica.sdk.adapter.javasdk.common.EReturnStatus;
import com.informatica.sdk.adapter.javasdk.common.Logger;
import com.informatica.sdk.adapter.javasdk.metadata.DataSourceOperationContext;
import com.informatica.sdk.adapter.javasdk.metadata.MetadataContext;
import com.informatica.sdk.adapter.metadata.common.datasourceoperation.semantic.iface.Capability;
import com.informatica.sdk.adapter.metadata.common.datasourceoperation.semantic.iface.WriteCapability;
import com.informatica.sdk.adapter.metadata.common.datasourceoperation.semantic.iface.ReadCapability;
import com.informatica.sdk.adapter.metadata.projection.helper.semantic.iface.BasicProjectionView;
import com.informatica.sdk.exceptions.SDKException;
import com.informatica.sdk.adapter.metadata.common.Status;
import com.informatica.sdk.adapter.metadata.common.StatusEnum;
import com.informatica.adapter.utils.runtime.impl.RuntimeUtilUserContext;
import com.informatica.sdk.adapter.javasdk.common.InfaUtils;
import com.informatica.adapter.onelake.metadata.adapter.OnelakeUtils;
import com.informatica.adapter.onelake.onelake.metadata.semantic.iface.SEMOnelakeRecordExtensions;
import com.informatica.adapter.onelake.onelake.runtime.capability.semantic.auto.SAOnelakeReadCapabilityAttributesExtension;
import com.informatica.adapter.sdkadapter.aso.runtime.D_ExecutionModeEnum;
import com.informatica.adapter.utils.runtime.IPartitionHelper;
import com.informatica.adapter.utils.runtime.RuntimeUtil;
import com.informatica.adapter.utils.runtime.impl.AdapterContext;
import com.informatica.adapter.utils.runtime.impl.AdapterUtils;
import com.informatica.adapter.utils.runtime.impl.FileType;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import com.informatica.sdk.adapter.metadata.patternblocks.flatrecord.semantic.iface.FlatRecord;
import com.informatica.sdk.adapter.metadata.patternblocks.shared.semantic.iface.ImportableObject;
import com.informatica.sdk.adapter.metadata.aso.semantic.iface.ASOOperation;
import com.informatica.sdk.adapter.metadata.capabilityattribute.semantic.iface.ReadCapabilityAttributes;

@SuppressWarnings("unused")
public class OnelakeOnelakeOperationAdapter extends DataSourceOperationAdapter  {

	private Logger logger = null;
	private InfaUtils infaUtils;
	private RuntimeUtilUserContext userContext = new RuntimeUtilUserContext();
	OnelakeOnelakeDataConnection conn;
	private D_ExecutionModeEnum currExecMode = null;
	private boolean isRecursiveRead=false;
	private boolean isAllowWildChars=false;
	private String connectionWorkSpaceName;
	private String workspaceNameForRead;
	private String connectionLakehousePath;
	private String filePathForRead;
	private boolean overridesHandledInMetadata = false;
	private boolean isDirectoryAsSrc=false;
	private String fileNameOverride = "";
	Boolean metadataFromRuntimeOverrideOnelake = true;

	public OnelakeOnelakeOperationAdapter(InfaUtils infaUtils){
		this.logger = infaUtils.getLogger(); 
		this.infaUtils=infaUtils;
	}



	/**
	 * This API should be implemented by adapter developer 
	 * to initialize the DSO adapter before any partitions are executed
	 *
	 * @param dsoHandle DSO handle
	 *		 This could be used to set any DSO level metadata common to all partitions in the user handle.
	 * @return EReturnStatus
	 */

	@Override
	public int initDataSourceOperation(DataSourceOperationContext dsoHandle, MetadataContext connHandle) throws SDKException  {
		int status = 0;
		dsoHandle.setUserHandle(userContext);
		BasicProjectionView projection = dsoHandle
				.getAdapterDataSourceOperation().getASOProjectionsList().get(0)
				.getProjectionHelper();
		Capability capability = dsoHandle.getAdapterDataSourceOperation()
				.getCapabilities().get(0);
		if (capability instanceof ReadCapability) {
			status = initDataOperationRead(dsoHandle, connHandle, projection);
		} else if (capability instanceof WriteCapability) {
			status = initDataOperationWrite(dsoHandle, connHandle, projection);
		}

		return status;
	}



	/**
	 * This API should be implemented by adapter developer 
	 * to de-initialize the DSO adapter after all partitions are executed
	 *
	 * @param dsoHandle DSO handle
	 *		This could be used to set any DSO level metadata common to all partitions in the user handle.
	 * @return EReturnStatus
	 */

	@Override
	public int deinitDataSourceOperation(DataSourceOperationContext dsoHandle, MetadataContext connHandle) throws SDKException  {
		int status = 0;
		BasicProjectionView projection = dsoHandle
				.getAdapterDataSourceOperation().getASOProjectionsList().get(0)
				.getProjectionHelper();
		Capability capability = dsoHandle.getAdapterDataSourceOperation()
				.getCapabilities().get(0);
		if (capability instanceof ReadCapability) {
			status = deinitDataOperationRead(dsoHandle, connHandle, projection);

		} else if (capability instanceof WriteCapability) {
			status = deinitDataOperationWrite(dsoHandle, connHandle, projection);
		}

		return status;
	}



	/**
	 * This API should be implemented by adapter developer 
	 * to perform read-specific pre-task
	 *
	 * @return EReturnStatus
	 */ 

	private int initDataOperationRead(DataSourceOperationContext dsoHandle, MetadataContext connHandle, BasicProjectionView projection)
	{
		try{
			ASOOperation m_asoOperation = dsoHandle.getAdapterDataSourceOperation();
			currExecMode = OnelakeUtils.getExecutionMode(m_asoOperation);
			boolean isSparkMode = OnelakeUtils.isSparkMode(currExecMode);
			boolean complexFileTypes=false;
			ASOOperation asoOperation = dsoHandle.getAdapterDataSourceOperation();
			if (!isSparkMode) {
				String resourceType = AdapterUtils.getResourceType(asoOperation);
				complexFileTypes = AdapterUtils.isComplexFileType(resourceType);
			}
			BasicProjectionView projectionView = asoOperation.getASOProjectionsList().get(0).getProjectionHelper();
			List<ImportableObject> nativeRecords = projectionView.getNativeRecords();
			FlatRecord fr = (FlatRecord) nativeRecords.get(0);
			ReadCapabilityAttributes capabilityAttributes = m_asoOperation.getReadCapabilityAttributes();
			SAOnelakeReadCapabilityAttributesExtension readAttribs = 
					(SAOnelakeReadCapabilityAttributesExtension) ((ReadCapabilityAttributes) capabilityAttributes)
					.getExtensions();

			String workspaceOverride = readAttribs.getWorkspaceOverride();
			String lakehouseOverride = readAttribs.getLakehouseOverride();
			fileNameOverride = readAttribs.getFileNameOverride();
			conn = new OnelakeOnelakeDataConnection(logger);
			conn.connect(connHandle);		
			connectionWorkSpaceName = conn.getMetadataConn().getWorkspaceName();
			connectionLakehousePath = conn.getMetadataConn().getLakehousePath();

			isRecursiveRead=readAttribs.getSourceType().equalsIgnoreCase("RecursiveDirectory") ? true : false;
			isAllowWildChars=readAttribs.isEnableWildChar();
			isDirectoryAsSrc = readAttribs.getSourceType().equalsIgnoreCase("File") ? false : true;	
			workspaceNameForRead = OnelakeUtils.getRuntimeWorkspaceName(connectionWorkSpaceName, workspaceOverride);

			FlatRecord sdkRecord = ( (FlatRecord) projectionView.getNativeRecords().get( 0 ) );
			SEMOnelakeRecordExtensions extn = (SEMOnelakeRecordExtensions) sdkRecord.getExtensions();
			this.overridesHandledInMetadata = extn.isOverridesHandledInMetadata();
			String recordNativeName = fr.getNativeName();		
			String recordName = OnelakeUtils.getRecordNameFromNativeName(recordNativeName);


			metadataFromRuntimeOverrideOnelake = Boolean.valueOf(System.getProperty("metadataFromRuntimeOverrideOnelake"));
			if (metadataFromRuntimeOverrideOnelake != null &&  !metadataFromRuntimeOverrideOnelake)
				this.logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_NORMAL,"Agent level flag [-DMetadataFromRuntimeOverrideOnelake] is set to handle overrides at runtime only.");

			if (!this.overridesHandledInMetadata) {
				this.logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_VERBOSE_DATA,"Overridden values [File Name Override, Lakehouse Override] were handled during runtime");
				filePathForRead = OnelakeUtils.getRuntimeFilePath(lakehouseOverride, fileNameOverride, recordNativeName,recordName);
			}
			else {
				this.logger.logMessage( EMessageLevel.MSG_INFO, ELogLevel.TRACE_VERBOSE_DATA,"Overridden values [File Name Override, Lakehouse Override] were handled during metadata");
				filePathForRead = OnelakeUtils.getRuntimeFilePathFilterByPath(lakehouseOverride, fileNameOverride, recordNativeName, isAllowWildChars);
			}
			
			filePathForRead = OnelakeUtils.validateObjectPath(filePathForRead, this.logger);

			if(complexFileTypes){
				initComplexFile();
			}	
		}
		catch(Exception e){
			logger.logMessage(EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE,"Reader Init Operation Failed :"+e.getMessage());
			throw new RuntimeException("Reader Init Operation Failed :"+e.getMessage());
		}
		return EReturnStatus.SUCCESS;
	}


	/**
	 * This API should be implemented by adapter developer 
	 * to perform write-specific pre-task
	 *
	 * @return EReturnStatus
	 */ 

	public void initComplexFile(){
		int recursiveDirectoryDepth;
		String directoryPath = null;
		String fileName = null;
		this.connectionWorkSpaceName = this.conn.getMetadataConn().getWorkspaceName();
		String complexFormatFilePath = "";
		String abfsFileSystemUrl = "abfss://" + this.workspaceNameForRead + "@"
				+ "onelake.dfs."
				+ this.conn.getMetadataConn().getEndPointSuffix();
		int lastIndexOfSlash = filePathForRead.lastIndexOf("/");
		if(lastIndexOfSlash != -1){
			fileName = filePathForRead.substring(lastIndexOfSlash+1);
			directoryPath = filePathForRead.substring(0,lastIndexOfSlash+1);
		}else{
			//this case can occur for files present at root workspace level, not inside any directory
			fileName = filePathForRead;
		}
		//complexFormatFilePath includes lakehouse path with filename.  abfsWorkspaceUrl includes only the workspaceName
		//complexFormatFilePath does not begin with /
		if(directoryPath != null){
			if(directoryPath.startsWith("/"))
				complexFormatFilePath = directoryPath.substring(1);
			else
				complexFormatFilePath = directoryPath;
		}	
		//directoryPath ends with / so directly append fileName
		if (!isDirectoryAsSrc ) 
			complexFormatFilePath += fileName;

		abfsFileSystemUrl+="/";	


		HashMap<String, String> confProperties = OnelakeUtils.getConfigProperties(conn.getMetadataConn());
		if (isRecursiveRead) {
			recursiveDirectoryDepth = -1;
		} else {
			recursiveDirectoryDepth = 0;
		}
		AdapterContext adapterContext = null;

		if ((isDirectoryAsSrc && (isAllowWildChars || isRecursiveRead)) && StringUtils.isNotEmpty(fileNameOverride)) {
			adapterContext = new AdapterContext.AdapterContextBuilder(this.infaUtils, abfsFileSystemUrl,
					complexFormatFilePath, FileType.Avro).setConfigProperties(confProperties).SecurityEnabled(false)
					.AllowWildChars(isAllowWildChars).setRecursiveDirectoryDepth(recursiveDirectoryDepth)
					.setFilePattern(fileNameOverride)
					.setAdapterClassloader(getClass().getClassLoader())
					.build();
			userContext.setFilePattern(fileNameOverride);
			logger.logMessage(EMessageLevel.MSG_INFO, ELogLevel.TRACE_NORMAL,
					String.format("File Name Pattern from overridden value: %s", fileNameOverride));

		}
		else
			adapterContext = new AdapterContext.AdapterContextBuilder(this.infaUtils, abfsFileSystemUrl,
					complexFormatFilePath, FileType.Avro).setConfigProperties(confProperties).SecurityEnabled(false)
			.AllowWildChars(isAllowWildChars).setRecursiveDirectoryDepth(recursiveDirectoryDepth)
			.setAdapterClassloader(getClass().getClassLoader())
			.build();

		IPartitionHelper partitionHelper = RuntimeUtil.createPartitionHelper();
		List<String> splits = partitionHelper.generateSerializedSplits(adapterContext, false);
		userContext.setListOfFileSplits(splits);
		logListOfFilesRead(splits);
	}

	private void logListOfFilesRead(List<String> splits) {
		if (splits.size() < 1)
			return;
		String msg;
		if (isAllowWildChars) {
			msg = String.format("WildChar is enabled. Found %s file[s] in workspace [%s] which match the provided wildcard pattern  %s ",
					splits.size(), workspaceNameForRead, filePathForRead);
		} else {
			msg = String.format("Found %s file[s] in workspace [%s] ", splits.size(),
					workspaceNameForRead);
		}
		logger.logMessage(EMessageLevel.MSG_INFO, ELogLevel.TRACE_NONE, msg);
		msg = splits.size() == 1 ? "File read [1] : " : String.format("List of files read [%s] : ", splits.size());

		logger.logMessage(EMessageLevel.MSG_INFO, ELogLevel.TRACE_VERBOSE_DATA, msg + "[ " + StringUtils.join(splits, ',') + " ]");

	}


	/**
	 * This API should be implemented by adapter developer 
	 * to perform write-specific pre-task
	 *
	 * @return EReturnStatus
	 */ 

	private int initDataOperationWrite(DataSourceOperationContext dsoHandle, MetadataContext connHandle, BasicProjectionView projection)
	{
		return EReturnStatus.SUCCESS;
	}



	/**
	 * This API should be implemented by adapter developer 
	 * to perform read-specific post-task
	 *
	 * @return EReturnStatus
	 */ 

	private int deinitDataOperationRead(DataSourceOperationContext dsoHandle, MetadataContext connHandle, BasicProjectionView projection)
	{  		
		return EReturnStatus.SUCCESS;
	}

	/**
	 * This API should be implemented by adapter developer 
	 * to perform write-specific post-task
	 *
	 * @return EReturnStatus
	 */ 

	private int deinitDataOperationWrite(DataSourceOperationContext dsoHandle, MetadataContext connHandle, BasicProjectionView projection)
	{
		return EReturnStatus.SUCCESS;
	}

}