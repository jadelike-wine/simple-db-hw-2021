package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    private static class Table{

        //用来表明类的不同版本间的兼容性
        private static final long serialVersionUID = 1L;
        public final DbFile dbFile;
        public final String tableName;
        public final String pkeyField;

        public Table(DbFile dbFile ,String tableName ,String pkeyField ){
            this.dbFile = dbFile;
            this.tableName = tableName;
            this.pkeyField = pkeyField;
        }

        @Override
        public String toString(){
            return tableName + "(" + dbFile.getId() + ":" + pkeyField + ")";
        }
    }

    private final ConcurrentHashMap<Integer,Table> hashTable;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        hashTable = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     *         将新表添加到目录中。
     *       * 此表的内容存储在指定的 DbFile 中。
     *       * @param file 要添加的表的内容； file.getId() 是此文件/tupledesc 参数用于调用 getTupleDesc 和 getFile
     *       * @param name 表的名称——可能是一个空字符串。 不能为空。 如果一个名字
     *       * 存在冲突，使用最后一个要添加的表作为给定名称的表。
     *       * @param pkeyField 主键字段的名称
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table t = new Table(file,name,pkeyField);
        hashTable.put(file.getId(),t);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     *
     * * 将新表添加到目录中。
     *       * 此表具有使用指定的 TupleDesc 格式化的元组及其
     *       * 内容存储在指定的 DbFile 中。
     *       * @param file 要添加的表的内容； file.getId() 是
     *       * 此文件/tupledesc 参数用于调用 getTupleDesc 和 getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     * 返回具有指定名称的表的id，
     *       * @throws NoSuchElementException 如果表不存在
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        for(Map.Entry<Integer,Table> entry : hashTable.entrySet()){
            if(entry.getValue().tableName.equals(name)){
                /**
                 * return entry.getKey();
                 * 这里我先写了getKey但是报错了，取dbFile的id确通过了，明明是同一个东西啊。
                 */
                return entry.getValue().dbFile.getId();
            }
        }
        throw new NoSuchElementException(name + " can not get in getTableId method");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     * 返回指定表的元组描述符（模式）
     *       * @param tableid 表的 id，由 DbFile.getId() 指定
     *       * 函数传递给 addTable
     *       * @throws NoSuchElementException 如果表不存在
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        Table t = hashTable.get(tableid);
        if(t == null){
            throw new NoSuchElementException("can not getTupleDesc by tableId " + tableid);
        } else {
            return t.dbFile.getTupleDesc();
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * 返回可用于读取文件内容的 DbFile
     *       * 指定表。
     *       * @param tableid 表的 id，由 DbFile.getId() 指定
     *       * 函数传递给 addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table t = hashTable.get(tableid);
        if(t == null){
            throw new NoSuchElementException("can not getDatabaseFile by tableId " + tableid);
        } else {
            return t.dbFile;
        }
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if(!hashTable.containsKey(tableid)){
            throw new NoSuchElementException("can not getPrimaryKey by tableId " + tableid);
        }
        return hashTable.get(tableid).pkeyField;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return hashTable.keySet().stream().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        if(!hashTable.containsKey(id)){
            throw new NoSuchElementException("can not getTableName by id " + id);
        }
        return hashTable.get(id).tableName;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        hashTable.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     * 从文件中读取模式并在数据库中创建适当的表。
     *       * @param 目录文件
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int")) {
                        types.add(Type.INT_TYPE);
                    } else if (els2[1].trim().equalsIgnoreCase("string")) {
                        types.add(Type.STRING_TYPE);
                    } else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        } else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

