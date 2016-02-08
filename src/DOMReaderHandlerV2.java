import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.ElementHandler;
import org.dom4j.ElementPath;
import org.dom4j.Namespace;
import org.dom4j.VisitorSupport;
import org.dom4j.io.SAXReader;
import org.dom4j.tree.DefaultElement;

public class DOMReaderHandlerV2 {
	static String outputFolder;

	static Logger log = Logger.getLogger(DOMReaderHandlerV2.class);

	static boolean parsingEnglish; // true if we parsing xml has english
									// documents, false other wise
	static HashMap<Integer, Integer> ID; // hash map contains id pairs of
											// documents
	static boolean acceptedPage = false; // boolean defines if the current pages
											// has a match or not
	static File file; // output file
	static FileWriter fw; // file writer to write the output file
	static String currentFileName; // the name of the current file/document

	static int pagesProcessed = 0; // number of pages processed
	static HashMap<Integer, Boolean> IdState = new HashMap<Integer, Boolean>();
	static HashMap<Integer, Integer> folderMap = new HashMap<Integer, Integer>();

	public static void Parser(String xmlInput, String _outputFolder, HashMap<Integer, Integer> IDs, boolean English,
			HashMap<Integer, Boolean> _IdState, HashMap<Integer, Integer> _folderMap) {
		try {

			parsingEnglish = English;
			ID = IDs;
			outputFolder = _outputFolder;
			IdState = _IdState;
			folderMap = _folderMap;
			
			SAXReader reader = new SAXReader();
			reader.addHandler("/mediawiki/page", new ElementHandler() {

				@Override
				public void onStart(ElementPath arg0) {
					// TODO Auto-generated method stub
				}

				@Override
				public void onEnd(ElementPath path) {
					pagesProcessed++;

					Element row = path.getCurrent();

					/*
					 * log.info(row.element("id").getStringValue());
					 * log.info(row.getName()); log.info(row.getStringValue());
					 * log.info(row.asXML());
					 */

					int f_id = Integer.parseInt(row.element("id").getStringValue());

					log.info("Processing Page Number " + pagesProcessed + " With ID " + f_id);

					// check if the id is matched from the data base
					if (ID.containsKey(f_id)) {
						IdState.put(f_id, true);
						log.info("Page with ID " + f_id + " matched the database");

						int s_id = ID.get(f_id);
						try {
							file = openFile(f_id, s_id, folderMap.get(f_id));
							fw = new FileWriter(file);

							log.info("Writing Page to file " + currentFileName);
							
							row.accept(new NameSpaceCleaner());
							
							fw.write(row.asXML());

							fw.flush();
							fw.close();
						} catch (IOException e) {
							log.fatal(e);
						}

					} else {
						log.info("Page with ID " + f_id + " didn't match the database.");
					}

					// prune the tree
					row.detach();
				}
			});

			URL url = new File(xmlInput).toURI().toURL();
			reader.read(url);

			File unmatched_id = new File("unmatched.o");
			file.createNewFile();
			FileWriter fw_umatched_id = new FileWriter(unmatched_id);
			Iterator<Entry<Integer, Boolean>> it = IdState.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Integer, Boolean> pair = (Map.Entry<Integer, Boolean>) it.next();
				if (!pair.getValue()) {
					fw_umatched_id.write(pair.getKey() + "\n");
				}
				it.remove(); // avoids a ConcurrentModificationException
			}
			fw_umatched_id.flush();
			fw_umatched_id.close();

		} catch (Exception e) {
			log.fatal(e);
		}

	}

	private static final class NameSpaceCleaner extends VisitorSupport {
		
		public void visit(Document document) {
			((DefaultElement) document.getRootElement()).setNamespace(Namespace.NO_NAMESPACE);
			document.getRootElement().additionalNamespaces().clear();
		}

		public void visit(Namespace namespace) {
			namespace.detach();
		}

		public void visit(Attribute node) {
			if (node.toString().contains("xmlns") || node.toString().contains("xsi:")) {
				node.detach();
			}
		}

		public void visit(Element node) {
			if (node instanceof DefaultElement) {
				((DefaultElement) node).setNamespace(Namespace.NO_NAMESPACE);
			}
		}

	}

	// open a file for the document to save
	protected static File openFile(int id_first, int id_second, int folder_map) throws IOException {
		
		String foldername = outputFolder + "/" + folder_map;

		// check if the directory is already exited, if not make one ^_*
		if (!Files.isDirectory(Paths.get(foldername))) {
			File folder = new File(foldername);
			folder.mkdirs();
		}

		String filename = "";

		if (parsingEnglish) {
			filename = foldername + "/en_" + id_first + ".xml";
		} else {
			filename = foldername + "/ar_" + id_first + ".xml";
		}

		currentFileName = filename;

		File file = new File(filename);
		file.createNewFile();

		return file;
	}

}
