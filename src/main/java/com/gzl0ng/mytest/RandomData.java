package com.gzl0ng.mytest;

import java.io.*;

/**
 * @author 郭正龙
 * @date 2021-11-30
 */
public class RandomData {
    public static void main(String[] args) throws IOException {
        String info = null;
        String path = RandomData.class.getResource("/").getPath();
        File file = new File(path + "salary.txt");
        System.out.println(path);
        FileWriter fw = new FileWriter(file, false);
        for (int i = 1; i < 1001; i++) {
            int n = (int) (Math.random() * 3);
            if (n == 0){
                Double salary = 5000 + Math.random() * (3001);
                String formatSalary = String.format("%.2f", salary);
                if (i == 1){
                    info = "FirstNamel" + " LastNamel" + " assistant " + formatSalary;
                }else {
                    info = "FirstName" + i + " LastName" + i + " assistant " + formatSalary;
                }
            }
            if (n == 1){
                Double salary = 60000 + Math.random() * (50001);
                String formatSalary = String.format("%.2f", salary);
                if (i == 1){
                    info = "FirstNamel" + " LastNamel" + " associate " + formatSalary;
                }else {
                    info = "FirstName" + i + " LastName" + i + " associate " + formatSalary;
                }
            }
            if (n == 2){
                Double salary = 75000 + Math.random() * (55000);
                String formatSalary = String.format("%.2f", salary);
                if (i == 1){
                    info = "FirstNamel" + " LastNamel" + " full " + formatSalary;
                }else {
                    info = "FirstName" + i + " LastName" + i + " full " + formatSalary;
                }
            }
            if (i < 1000){
                fw.write(info + "\n");
                continue;
            }
            fw.write(info);
        }
        fw.close();
    }
}
