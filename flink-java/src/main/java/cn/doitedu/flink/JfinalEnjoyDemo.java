package cn.doitedu.flink;

import com.jfinal.template.Engine;
import com.jfinal.template.Template;

import java.util.HashMap;

/**
 * @projectName: flink-java
 * @package: cn.doitedu.flink
 * @className: JfinalEnjoyDemo
 * @author: Cypress_Xiao
 * @description: TODO
 * @date: 2022/9/16 14:34
 * @version: 1.0
 */

public class JfinalEnjoyDemo {
    public static void main(String[] args) {
        String s ="hello\n" +
                "#if(name!=null)\n" +
                "你好:#(name)\n" +
                "#end";
        Template template = Engine.use().getTemplateByString(s);
        HashMap<String, Object> map = new HashMap<>();
        map.put("name","张三");
        String s1 = template.renderToString(map);
        System.out.println(s1);
    }
}
