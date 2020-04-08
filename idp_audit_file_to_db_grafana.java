package filetodb;

//import com.mysql.jdbc.Connection;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import javax.annotation.Resource;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author sia
 */
public class idp_audit_file_to_db_grafana {
        private File file;
        private File configurationfile;
        
        private FileReader filereader=null;
        private FileReader configurationfilereader=null;
        private BufferedReader br;
        private BufferedReader configurationbr;
        private Connection conn;
        private String nombre_y_direccion_del_fichero=null;
        private String driver="com.mysql.jdbc.Driver";
        private String user="root";
        private String password="";
        private String ip_bd;
        private String puerto_bd;
        private String url= "jdbc:mysql://";//"jdbc:mysql://localhost:3306";
        private String lineaconfiguracion;
        private String linea;
        private String ultima_linea_leida;
        private String offset_hora;
        
        
    public void filetodb()
    {
       
        
        System.out.println("Ejecutando programa"); 
        
        
        try {
    
         leer_fichero_configuracion();
         
         leer_fichero_idp_audit_guardar_a_bd();
         
      }
      catch(Exception e){
         e.printStackTrace();
      }finally{
         // En el finally cerramos el fichero, para asegurarnos
         // que se cierra tanto si todo va bien como si salta 
         // una excepcion.
            cerrar_conexiones();
        

    }
    
}
    
    private void leer_fichero_configuracion() throws Exception 
    {
        //Lectura del fichero de configuracion
        
        configurationfile = new File (getRutaRecurso("Fichero-configuracion-Idp-audit.txt"));
        configurationfilereader=new FileReader(configurationfile);
        configurationbr= new BufferedReader(configurationfilereader);
        
        while((lineaconfiguracion=configurationbr.readLine())!=null)
         {
         System.out.println("Leyendo fichero de configuracion");
         System.out.println(lineaconfiguracion);
         
            String[] elementoslinea; 
            
            elementoslinea = lineaconfiguracion.split(":",2);
            
            int i=0;
            for (String a : elementoslinea) 
            {
            if (i==0)
            {
            if (a.equals("DIRECCION Y NOMBRE DEL FICHERO IDP-AUDIT-FILE"))
                    {
                      nombre_y_direccion_del_fichero=elementoslinea[1];  
                      System.out.println("direccion_fichero"+nombre_y_direccion_del_fichero);
                      if (nombre_y_direccion_del_fichero.length()==0)
                      {
                          System.out.println("El fichero de configuracion tiene el campo DIRECCION Y NOMBRE DEL FICHERO IDP-AUDIT-FILE vacio");
                          System.exit(0);
                      }
                    }
            else if (a.equals("IP DE LA BASE DE DATOS"))
                    {
                      ip_bd=elementoslinea[1];  
                      System.out.println("ip_bd "+ip_bd);
                      if (ip_bd.length()==0)
                      {
                          System.out.println("El fichero de configuracion tiene el campo IP DE LA BASE DE DATOS vacio");
                          System.exit(0);
                      }
                    }
            else if (a.equals("PUERTO DE LA BASE DE DATOS"))
                    {
                      puerto_bd=elementoslinea[1];  
                      System.out.println("puerto_bd "+puerto_bd);
                       if (puerto_bd.length()==0)
                      {
                          System.out.println("El fichero de configuracion tiene el campo PUERTO DE LA BASE DE DATOS vacio");
                          System.exit(0);
                      }
                      
                    }
            else if (a.equals("USUARIO DE LA BASE DE DATOS"))
                    {
                      user=elementoslinea[1];  
                      System.out.println("user "+user);
                      
                      if (user.length()==0)
                      {
                          System.out.println("El fichero de configuracion tiene el campo USUARIO DE LA BASE DE DATOS vacio");
                          System.exit(0);
                      }
                      
                    }
            else if (a.equals("CONSTRASEÑA DEL USUARIO DE LA BASE DE DATOS"))
                    {
                      password=elementoslinea[1];  
                      System.out.println("password "+password);
                    }
            else if (a.equals("AÑADIR X HORAS"))
                    {
                      offset_hora=elementoslinea[1];  
                      System.out.println("offset_horas " +offset_hora);
                    }
             else if (a.equals("ULTIMA LINEA LEIDA DEL FICHERO"))
                    {
                      this.ultima_linea_leida=elementoslinea[1];  
                      System.out.println("offset_horas " +ultima_linea_leida);
                    }
            }    
            System.out.println(a); 
            i++;
            }  
         
         }
    }
    
    
    private void leer_fichero_idp_audit_guardar_a_bd()throws Exception
         
    {

        //file = new File ("/home/sia/Descargas/idp-audit.log");
        file = new File (this.nombre_y_direccion_del_fichero);
        
        filereader=new FileReader(file);
            
        // Apertura del fichero y creacion de BufferedReader para poder
         
        br = new BufferedReader(filereader);
        url =url+ip_bd+":"+puerto_bd;
        conn = DriverManager.getConnection(url,user, password);
        if (conn!=null)
        {
            System.out.println("Conexion establecida");
        }
        
         // Lectura del fichero
         Statement st = conn.createStatement();   
         int eventid=1; 
          while((linea=br.readLine())!=null && (linea.equals(ultima_linea_leida))==false)
         {
             
            System.out.println(linea);
            System.out.println("ANALIZANDO LINEA");
            String[] elementosString; 
            
            elementosString = linea.split("\\|",14);
            int i=0;
            for (String a : elementosString) 
            {
            System.out.println(a); 
            i++;
            } 
            
            System.out.println("elementos linea:"+i); 
            System.out.println("Instertando elementos a BD");
            
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
            //formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
            //formatter.setTimeZone(TimeZone.getTimeZone("Europe/London"));
            java.util.Date dateStr = formatter.parse(elementosString[0]);
            java.sql.Date dateDB = new java.sql.Date(dateStr.getTime());
            
            st.executeUpdate("INSERT INTO PRUEBA_GRAFANA.accessos (time,eventid,sessionid, aplicacio,username) VALUES ('"+dateDB+"','"+eventid+"','"+elementosString[2]+"','"+elementosString[3]+"','"+elementosString[8]+"' )");
            
            ultima_linea_leida=linea;
            
         }
       //configurationfile = new File (getRutaRecurso("Fichero-configuracion-Idp-audit.txt"));
       //configurationfilereader=new FileReader(configurationfile);
       //configurationbr= new BufferedReader(configurationfilereader);
       //while((lineaconfiguracion=configurationbr.readLine())!=null)
         
          BufferedWriter bw = new BufferedWriter(new FileWriter(this.configurationfile));
          
    }
            
   
    
     /**
 * Obtenemos la ruta hasta el fichero , habitualmente situado junto al
 * archivo JAR. En caso contrario, estariamos ejecutando desde un IDE y
 * buscaremos el archivo junto al pom.xml y las carpetas target y src.
 *
 * @param filename nombre fle fichero(con extension) a abrir
 * @return Ruta completa hasta el fichero
 * @throws URISyntaxException
 * @throws IOException
 */
    public static String getRutaRecurso(String filename) throws URISyntaxException, IOException {
    //final ProtectionDomain domain;
    final CodeSource source;
    final URL url;
    final URI uri;
    String DirectoryPath;
    String separador_directorios=System.getProperty("file.separator");
    String JarURL;
    File auxiliar;
     final ProtectionDomain domain = idp_audit_file_to_db_grafana.class.getProtectionDomain();
    source = domain.getCodeSource();
    url = source.getLocation();
    uri = url.toURI();
    JarURL = uri.getPath();
    auxiliar = new File(JarURL);
    //Si es un directorio es que estamos ejecutando desde el IDE. En este caso
    // habrá que buscar el fichero en la carperta  abuela(junto a las carpetas "src" y "target·
    if (auxiliar.isDirectory()) {
        auxiliar = new File(auxiliar.getParentFile().getParentFile().getPath());
        DirectoryPath = auxiliar.getCanonicalPath() + separador_directorios;
    } else {
        JarURL=auxiliar.getCanonicalPath();
        DirectoryPath = JarURL.substring(0, JarURL.lastIndexOf(separador_directorios) + 1);

    }

    System.out.println(DirectoryPath + filename);
    return DirectoryPath + filename;
}

    private void cerrar_conexiones()
    {
         try{                    
            if( filereader!=null ){   
               filereader.close();     
            }                  
         }catch (Exception e2){ 
            e2.printStackTrace();
         }
        
    }
    
    
}
