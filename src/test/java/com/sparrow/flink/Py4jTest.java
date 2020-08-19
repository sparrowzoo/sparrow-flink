package com.sparrow.flink;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

public class Py4jTest {
    public static void main(String[] args) throws IOException, InterruptedException {
//        Py4jTest app = new Py4jTest();
//        GatewayServer server = new GatewayServer(app);
//        server.start();

        List commands = new java.util.ArrayList<String>();
        commands.add("python");
        commands.add("-m");
        commands.add("main");
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
        Map workerEnv = processBuilder.environment();
        /**
         * python -h 环境变量
         * Other environment variables:
         * PYTHONSTARTUP: file executed on interactive startup (no default)
         * PYTHONPATH   : ';'-separated list of directories prefixed to the
         *                default module search path.  The result is sys.path.
         * PYTHONHOME   : alternate <prefix> directory (or <prefix>;<exec_prefix>).
         *                The default module search path uses <prefix>\python{major}{minor}
         */
        workerEnv.put("PYTHONPATH", "D:\\workspace\\sparrow\\sparrow-flink\\src\\test\\resources\\python\\sparrow-src.zip");
        Process worker = processBuilder.start();


        worker.waitFor();

        // 定义Python脚本的返回值
        String result = null;
        // 获取CMD的返回流
        BufferedInputStream in = new BufferedInputStream(worker.getInputStream());
        // 字符流转换字节流
        BufferedReader br = new BufferedReader(new InputStreamReader(in,"UTF-8"));

        // 这里也可以输出文本日志

        String lineStr = null;
        while ((lineStr = br.readLine()) != null) {
            result = lineStr;
        }
        // 关闭输入流
        br.close();
        in.close();

        System.out.println(result);
    }

    public int addition(int first, int second) {
        return first + second;
    }
}
