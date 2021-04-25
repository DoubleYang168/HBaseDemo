package cn.edu.hhu.dingy.directory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TraverseDirectoryFiles {

    public static void main(String[] args) {
        List<String> list = nonRecursionTraverseFolder("C:\\Users\\dingy\\Desktop\\hdf5_files\\hdf5_files\\evaporation2010-2018");
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i).split("\\\\")[7].replace(".h5", ""));
        }
    }

    /**
     * 非递归遍历文件夹中的文件
     *
     * @param path
     */
    public static List<String> nonRecursionTraverseFolder(String path) {
        int fileNum = 0, folderNum = 0;
        File file = new File(path);
        List<String> filesList = new ArrayList<>();
        if (file.exists()) {
            LinkedList<File> list = new LinkedList<File>();
            File[] files = file.listFiles();
            for (File file2 : files) {
                if (file2.isDirectory()) {
                    System.out.println("文件夹:" + file2.getAbsolutePath());
                    list.add(file2);
                    folderNum++;
                } else {
                    System.out.println("文件:" + file2.getAbsolutePath());
                    filesList.add(file2.getAbsolutePath());
                    fileNum++;
                }
            }
            File temp_file;
            while (!list.isEmpty()) {
                temp_file = list.removeFirst();
                files = temp_file.listFiles();
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        list.add(file2);
                        folderNum++;
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                        fileNum++;
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
        System.out.println("文件夹共有:" + folderNum + ",文件共有:" + fileNum);
        return filesList;
    }

    /**
     * 递归遍历文件夹中的文件
     *
     * @param path
     */
    public void recursionTraverseFolder(String path) {

        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files.length == 0) {
                System.out.println("文件夹是空的!");
                return;
            } else {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        System.out.println("文件夹:" + file2.getAbsolutePath());
                        recursionTraverseFolder(file2.getAbsolutePath());
                    } else {
                        System.out.println("文件:" + file2.getAbsolutePath());
                    }
                }
            }
        } else {
            System.out.println("文件不存在!");
        }
    }
}
