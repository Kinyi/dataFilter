package cn.adsage.dc.dataFilter;


public class Test {

	@org.junit.Test
	public void test() {
		String str = "20150614000001 4C1FCC6D4247B4E8210A  iPhone7,1 8.3 1 139430000 139430600";
		String[] split = str.replaceAll("\\s+", ";").split(";");
		System.out.println(split.length);
	}

}
