package net.uresk.samples.elastic.aliasing;

public class IndexOperationResponse<T>
{

	T result;
	
	boolean success;
	
	public IndexOperationResponse(boolean success)
	{
		this.success = success;
	}
	
	public IndexOperationResponse(boolean success, T result)
	{
		this.success = success;
		this.result = result;
	}

	public T getResult()
	{
		return result;
	}

	public boolean isSuccess()
	{
		return success;
	}
	
}
