package com.informatica.adapter.onelake.runtime.adapter;

import com.informatica.sdk.adapter.parser.runtime.iface.OutputData;

public class OnelakeOutputData implements OutputData
{

	Object obj;

	@Override
	public void setData(Object data) {
		obj = data;
		
	}
	
	public Object getData() {
		return obj;
	}
	
}
