/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package filetodb;

import java.io.File;

/**
 *
 * @author sia
 */
public class JavaApplication1 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        System.out.println("main");
        idp_audit_file_to_db_grafana filetodb=new idp_audit_file_to_db_grafana();
        
        filetodb.filetodb();
        
    }
    
    
}
