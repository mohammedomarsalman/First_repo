package com.informatica.adapter.onelake.runtime.adapter;

import com.informatica.sdk.adapter.javasdk.dataadapter.Connection;
import java.util.Map;
import java.util.HashMap;
import com.informatica.sdk.adapter.javasdk.common.ELogLevel;
import com.informatica.sdk.adapter.javasdk.common.EMessageLevel;
import com.informatica.sdk.adapter.javasdk.common.EReturnStatus;
import com.informatica.sdk.adapter.javasdk.common.Logger;
import com.informatica.sdk.adapter.javasdk.metadata.MetadataContext;
import com.informatica.sdk.adapter.metadata.common.Status;
import com.informatica.sdk.adapter.metadata.common.StatusEnum;
import com.informatica.sdk.exceptions.SDKException;
import com.informatica.adapter.onelake.metadata.adapter.OnelakeConnection;

public class OnelakeOnelakeDataConnection extends Connection  {

    private Logger logger = null;

    private OnelakeConnection metadataConn = new OnelakeConnection();

    public OnelakeOnelakeDataConnection(Logger infaLogger){
     	this.logger = infaLogger; 
     	this.metadataConn= new OnelakeConnection(infaLogger);
    }



    /**
    * Connects to the external data source. This method reuses the metadata connection to connect to the data source. 
    * Optionally, override this method if you want use a connection that is different from the metadata connection.
    * @param connHandle The connection handle.
    * @return An integer value defined in the EReturnStatus class that indicates the status of the connect operation.
    * @throws SDKException
    */

    public int connect(MetadataContext connHandle){
         Map<String,Object> attrMap = new HashMap<String,Object>();

         try{
          	attrMap.put("clientID", connHandle.getStringAttribute("clientID"));
          	attrMap.put("clientSecret", connHandle.getStringAttribute("clientSecret"));
          	attrMap.put("tenantID", connHandle.getStringAttribute("tenantID"));
          	attrMap.put("workspaceName", connHandle.getStringAttribute("workspaceName"));
          	attrMap.put("lakehousePath", connHandle.getStringAttribute("lakehousePath"));
          	attrMap.put("endPointSuffix", connHandle.getStringAttribute("endPointSuffix"));
          	attrMap.put("authenticationType", connHandle.getStringAttribute("authenticationType"));
			attrMap.put("productInfo", connHandle.getStringAttribute("productInfo"));
			
           	Status status = metadataConn.openConnection(attrMap);
           	if(status.getStatus() == StatusEnum.SUCCESS){
           		return EReturnStatus.SUCCESS;
           	}else {
           		String msg = status.getMessage();
           		if(msg != null){
           			logger.logMessage(EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, msg);
           		}
           		return EReturnStatus.FAILURE;
           	}

         } catch (SDKException e) {
        	logger.logMessage(EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, e.getMessage());
        }

        return EReturnStatus.FAILURE;
    }



    /**
    * Closes the connection with the external data source. This method reuses the metadata connection to disconnect from the data source. 
    * Optionally, override this method if you want use a connection that is different from the metadata connection.
    * @param connHandle The connection handle.
    * @return An integer value defined in the EReturnStatus class that indicates the status of the disconnect operation.
    */                           


    public int disConnect(MetadataContext connHandle){
        Status status = metadataConn.closeConnection();
        if(status.getStatus() == StatusEnum.SUCCESS){
        	return EReturnStatus.SUCCESS;
        }else {
        	String msg = status.getMessage();
        	if(msg != null){
        		logger.logMessage(EMessageLevel.MSG_ERROR, ELogLevel.TRACE_NONE, msg);
        	}
        	return EReturnStatus.FAILURE;
        }
    }



    /**
    * Gets the connection with the external data source. This method reuses the metadata connection to connect to the data source. 
    * Optionally, override this method if you want use a connection that is different from the metadata connection.
    * @return An integer value defined in the EReturnStatus class that indicates the status of the get operation.
    */                           
    
    public OnelakeConnection getMetadataConn() {
		return metadataConn;
	}


}