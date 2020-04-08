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
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
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
        private File ultima_linea_file=null;
        
        private FileReader filereader=null;
        private FileReader configurationfilereader=null;
        private FileReader ultimafilafilereader=null;
        private FileWriter ultima_linea_file_writer=null;
        
        private BufferedReader br;
        private BufferedReader configurationbr;
        private BufferedReader ultima_linea_br;
        private BufferedWriter bw ;
        private Connection conn;
        private Statement st;
        private ResultSet rs;
        private String nombre_y_direccion_del_fichero=null;
        private String driver="com.mysql.jdbc.Driver";
        //private String driver2="ojdbc7";
        private String user="root";
        private String password="";
        private String ip_bd;
        private String puerto_bd;
        private String url= "jdbc:mysql://";//"jdbc:mysql://localhost:3306";
        private String lineaconfiguracion;
        private String linea;
        private String ultima_linea_leida;
        private String offset_hora;
        private String query;
        private int numero_ultima_linea_leida;
        private Boolean b_despres_05=false;
        private java.sql.Timestamp dateDB;
        private String nombre_bd;        

    public idp_audit_file_to_db_grafana() {
    }
        
        
        
    public void filetodb()
    {
       
        
        System.out.println("Ejecutando programa"); 
        
        
        try {
           
         System.out.println("Leer fichero configuracion");   
         leer_fichero_configuracion();
         
         System.out.println("Leer fichero ultima linea");   
         leer_fichero_ultima_linea();
         
         System.out.println("Leer fichero idp audit a guardar a bd");
         System.out.println(LocalTime.now());
         ZoneId z=ZoneId.of("Europe/Berlin");
          
         LocalTime localtime = LocalTime.now(z);
         System.out.println(localtime);
         LocalTime time1 
            = LocalTime.parse("00:05:00"); 
        LocalTime time2 
            = LocalTime.parse("00:00:00"); 
        
        if ((localtime.isAfter(time2))&&(localtime.isBefore(time1)))
        {
            //SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

// Esto muestra la fecha actual en pantalla, más o menos así 28/03/2017
           // System.out.println(sdf.format(new Date()));
            
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
            String fechaComoCadena = sdf.format(new Date());
            String nombreaux=nombre_y_direccion_del_fichero;   
            this.nombre_y_direccion_del_fichero=nombre_y_direccion_del_fichero+"-"+fechaComoCadena;
            
            leer_fichero_idp_audit_guardar_a_bd();
            numero_ultima_linea_leida=0;
            this.nombre_y_direccion_del_fichero=nombreaux;
            this.b_despres_05=true;
        }
        
         //caso normal   
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
    private void leer_fichero_ultima_linea()throws Exception
    {
        System.out.println("LEER FICHERO ULTIMA LINEA");
        ultima_linea_file=new File(getRutaRecurso("ultima-linea-leida-fichero-proyecto-grafana.txt"));
       // System.out.println(getRutaRecurso("ultima-linea-leida-fichero-proyecto-grafana.txt"));
       
         this.ultimafilafilereader=new FileReader(ultima_linea_file);
        
        ultima_linea_br=new BufferedReader(this.ultimafilafilereader);
        String s_numero_ultima_linea_leida; 
        int nl=0;
        this.numero_ultima_linea_leida=0;
        String s_ul;
      while((s_ul=ultima_linea_br.readLine())!=null)
         {
           if (nl==0)
           {
               //ultima_linea_br. .getLineNumber()==1){
               ultima_linea_leida=s_ul;
           }
           else if  (nl==1)
           {
           //ultima_linea_leida=s_ul;
           s_numero_ultima_linea_leida=s_ul;
           this.numero_ultima_linea_leida=Integer.parseInt(s_numero_ultima_linea_leida);
           }
           nl++;
         }
      System.out.println("ultima_linea_leida");
      System.out.println(ultima_linea_leida);
      System.out.println("this.numero_ultima_linea_leida");
      System.out.println(this.numero_ultima_linea_leida);
      System.out.println("fin ");  
      

            
        
    }
    
    private void leer_fichero_configuracion() throws Exception 
    {
        //Lectura del fichero de configuracion
       
        configurationfile = new File (getRutaRecurso("Fichero-configuracion-Idp-audit.txt"));
        //ultima_linea_file=new File(getRutaRecurso("ultima-linea-leida-fichero-proyecto-grafana.txt"));
        //ultima_linea_file=new File("/home/sia/NetBeansProjects/Idp-audit-file-to-db-grafana/ultima-linea-leida-fichero-proyecto-grafana.txt");
        
        configurationfilereader=new FileReader(configurationfile);
        configurationbr= new BufferedReader(configurationfilereader);
        
         //String content = "This is the content to write into file\n";
        //this.ultima_linea_file_writer= new FileWriter(getRutaRecurso("ultima-linea-leida-fichero-proyecto-grafana.txt"));
       
        
                

        //bw.write(content);
        //bw.close();
        
        
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
            else if (a.equals("NOMBRE DE LA TABLA"))
                    {
                      nombre_bd=elementoslinea[1];  
                      System.out.println("nombre_bd "+nombre_bd);
                      
                      if (nombre_bd.length()==0)
                      {
                          System.out.println("El fichero de configuracion tiene el campo NOMBRE DE LA BASE DE DATOS vacio");
                          System.exit(0);
                      }
                      
                    }
            
            else if (a.equals("CONTRASENA DEL USUARIO DE LA BASE DE DATOS"))
                    {
                      password=elementoslinea[1];  
                      System.out.println("password "+password);
                    }
            else if (a.equals("HORAS"))
                    {
                      offset_hora=elementoslinea[1];  
                      System.out.println("offset_horas " +offset_hora);
                    }
            
             /*else if (a.equals("ULTIMA LINEA LEIDA DEL FICHERO"))
                    {
                      this.ultima_linea_leida=elementoslinea[1];  
                      System.out.println("ultima_linea_leida" +ultima_linea_leida);
                    }
            */
            }    
            System.out.println(a); 
            i++;
            }  
         
         }
    }
    
    
    private void leer_fichero_idp_audit_guardar_a_bd()throws Exception
         
    {
        //getRutaRecurso(

        //file = new File ("/home/sia/Descargas/idp-audit.log");
        
        System.out.println("LEER FICHERO IDP_AUDIT GUARDAR A BD");
        System.out.println("nd "+this.nombre_y_direccion_del_fichero);
        file = new File (getRutaRecurso(this.nombre_y_direccion_del_fichero));
       // this.numero_ultima_linea_leida=0;
        
        filereader=new FileReader(file);
            
        // Apertura del fichero y creacion de BufferedReader para poder
        
        br = new BufferedReader(filereader);
        url =url+ip_bd+":"+puerto_bd;
        System.out.println(url);
        System.out.println(user);
        System.out.println(password);
        
        
        if (this.b_despres_05==false)
        {
        conn = DriverManager.getConnection(url,user, password);
        //System.exit(0);
        if (conn!=null)
        {
            System.out.println("Conexion establecida");
        }
        this.b_despres_05=true;
        }
         // Lectura del fichero
          st = conn.createStatement();   
        
          int eventid=1; 
         
         String select_query = "select id,data,eventid,sessionid, aplicacio,username " +
                   "from "+nombre_bd+ " order by id desc limit 1";
         
          rs = st.executeQuery(select_query);
         
         int topBD_id=0;
         String topdata="20200331T094759Z";
         SimpleDateFormat topformatter = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
         java.util.Date datetopStr = topformatter.parse(topdata);
         java.sql.Timestamp datetopDB = new java.sql.Timestamp(datetopStr.getTime());
           
         java.sql.Timestamp topBD_time=  datetopDB;
         int topBD_eventid=0;
         String topBD_sessionid="";
         String topBD_aplicacio="";
         String topBD_username="";
         
         while (rs.next())
           {
         
            topBD_id=rs.getInt("id");
            topBD_time = rs.getTimestamp("data");
            topBD_eventid = rs.getInt("eventid");
            topBD_sessionid = rs.getString("sessionid");
            topBD_aplicacio = rs.getString("aplicacio");
            topBD_username = rs.getString("username");
            
           // System.out.println(topBD_id);
            System.out.println(topBD_time);
            System.out.println(topBD_eventid);
            System.out.println(topBD_sessionid);
            System.out.println(topBD_aplicacio);
            System.out.println(topBD_username);
           }
            
        
         //  System.out.println(this.topBD_time + this.topBD_eventid + this.topBD_sessionid +  this.topBD_aplicacio + this.topBD_username);
        boolean b_linea_repetida=false;
         
         //System.exit(0);
         // while((linea=br.readLine())!=null && (linea.equals(ultima_linea_leida))==false)
         int i_linea=0;
          // while(((linea=br.readLine())!=null) && (b_linea_repetida==false))
         while(((linea=br.readLine())!=null))
         {
            if (i_linea<this.numero_ultima_linea_leida)
            {
                System.out.println(i_linea);
                i_linea++;
            }
            else
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
            //formatter.setTimeZone(TimeZone.getTimeZone("Europe/London"));S
            java.util.Date dateStr = formatter.parse(elementosString[0]);
            System.out.println(dateStr);
            
           
            //java.sql.Date dateDB = new java.sql.Date(dateStr.getTime());
             dateDB=new java.sql.Timestamp(dateStr.getTime());
            System.out.println(dateDB);
            
            System.out.println(topBD_time);
            System.out.println(dateDB);
            //se comrueba que la linea que se va insertar a la bd no era la primera que habia antes.
           /* if ((elementosString[8].equals(topBD_username)) && (elementosString[3].equals(topBD_aplicacio))&& (elementosString[2].equals(topBD_sessionid))  && (eventid==topBD_eventid) && (dateDB.equals(topBD_time)))// &&)
                     {
                      System.out.println("dateDB"+dateDB);
                      System.out.println("topBD"+topBD_time);
                      System.out.println("LINEA REPETIDA");
                      
                      b_linea_repetida=true; 
                      
                     }
             else{*/
              aplicar_offset_hora();    
               System.out.println(dateDB);
              System.out.println("LINEA NO REPETIDA");
              st.executeUpdate("INSERT INTO "+nombre_bd+" (data,eventid,sessionid, aplicacio,username) VALUES ('"+dateDB+"','"+eventid+"','"+elementosString[2]+"','"+elementosString[3]+"','"+elementosString[8]+"' )");
             //}
            ultima_linea_leida=linea;
            this.numero_ultima_linea_leida++;
            i_linea++;
            }
         }
       //configurationfile = new File (getRutaRecurso("Fichero-configuracion-Idp-audit.txt"));
       //configurationfilereader=new FileReader(configurationfile);
       //configurationbr= new BufferedReader(configurationfilereader);
       //while((lineaconfiguracion=configurationbr.readLine())!=null)
          this.ultima_linea_file_writer= new FileWriter(ultima_linea_file);
          bw = new BufferedWriter(this.ultima_linea_file_writer);
          bw.write(ultima_linea_leida+"\n"+this.numero_ultima_linea_leida);
          
           
    }
            
   public void aplicar_offset_hora()
   {
       
       System.out.println(dateDB);
       boolean resta=false;
       if (offset_hora.contains(":00"))
       {
           offset_hora=offset_hora.replace(":00", "");
       }
       
      if (offset_hora.contains("-")){
      resta=true;
       offset_hora=offset_hora.replace("-", "");
      }else if (offset_hora.contains("+")){
       offset_hora=offset_hora.replace("-", "");   
      }
      
      int offset;  
      
      offset=Integer.parseInt(offset_hora);
      
       offset=offset*60* 60 * 1000;

      if (resta==false)
      {
      dateDB.setTime(dateDB.getTime()+offset);
      }
      else
      {     
      dateDB.setTime(dateDB.getTime()-offset);
      }
       //dateDB
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
               this.br.close();
            }
            
            
               this.configurationbr.close();
               this.configurationfilereader.close();
               
               if (bw!=null)
               {
               bw.close();
               }
               
               if (ultima_linea_file_writer!=null)
               {
               ultima_linea_file_writer.close();
               }
               if (rs!=null)
               {
               rs.close();
                       }
               if (st!=null)
               {
                   st.close();
               }
               if (conn!=null)
               {
               conn.close();
               }
               
               this.ultima_linea_br.close();
               this.ultimafilafilereader.close();
               
               //this.filereader.close();
              
               
            //}                  
         }catch (Exception e2){ 
            e2.printStackTrace();
         }
        
    }
    
    
}
