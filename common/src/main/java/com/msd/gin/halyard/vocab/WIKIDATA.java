package com.msd.gin.halyard.vocab;

import com.msd.gin.halyard.common.AbstractIRIEncodingNamespace;
import com.msd.gin.halyard.common.ValueIO;
import com.msd.gin.halyard.common.Vocabulary;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(Vocabulary.class)
public final class WIKIDATA implements Vocabulary {
    private static final SimpleValueFactory SVF = SimpleValueFactory.getInstance();

    public static final Namespace WDATA_NS = new EntityNamespace("wdata", "https://www.wikidata.org/wiki/Special:EntityData/");
    public static final String WD_NAMESPACE = "http://www.wikidata.org/entity/";
    public static final String WDS_NAMESPACE = "http://www.wikidata.org/entity/statement/";
    public static final String WDV_NAMESPACE = "http://www.wikidata.org/value/";
    public static final String WDT_NAMESPACE = "http://www.wikidata.org/prop/direct/";
    public static final String WDTN_NAMESPACE = "http://www.wikidata.org/prop/direct-normalized/";
    public static final String WDNO_NAMESPACE = "http://www.wikidata.org/prop/novalue/";
    public static final Namespace WD_NS = new EntityNamespace("wd", WD_NAMESPACE);
    public static final Namespace WDS_NS = new StatementNamespace("wds", WDS_NAMESPACE);
    public static final Namespace WDV_NS = new HashNamespace("wdv", WDV_NAMESPACE);
    public static final Namespace WDREF_NS = new HashNamespace("wdref", "http://www.wikidata.org/reference/");
    public static final Namespace WDT_NS = new EntityNamespace("wdt", WDT_NAMESPACE);
    public static final Namespace WDTN_NS = new EntityNamespace("wdtn", WDTN_NAMESPACE);
    public static final Namespace WDNO_NS = new EntityNamespace("wdno", WDNO_NAMESPACE);

    public static final String P_NAMESPACE = "http://www.wikidata.org/prop/";
    public static final String PS_NAMESPACE = "http://www.wikidata.org/prop/statement/";
    public static final String PSV_NAMESPACE = "http://www.wikidata.org/prop/statement/value/";
    public static final String PSN_NAMESPACE = "http://www.wikidata.org/prop/statement/value-normalized/";
    public static final String PQ_NAMESPACE = "http://www.wikidata.org/prop/qualifier/";
    public static final String PQV_NAMESPACE = "http://www.wikidata.org/prop/qualifier/value/";
    public static final String PQN_NAMESPACE = "http://www.wikidata.org/prop/qualifier/value-normalized/";
    public static final String PR_NAMESPACE = "http://www.wikidata.org/prop/reference/";
    public static final String PRV_NAMESPACE = "http://www.wikidata.org/prop/reference/value/";
    public static final String PRN_NAMESPACE = "http://www.wikidata.org/prop/reference/value-normalized/";

    public static final Namespace P_NS = new EntityNamespace("p", P_NAMESPACE);
    public static final Namespace PS_NS = new EntityNamespace("ps", PS_NAMESPACE);
    public static final Namespace PSV_NS = new EntityNamespace("psv", PSV_NAMESPACE);
    public static final Namespace PSN_NS = new EntityNamespace("psn", PSN_NAMESPACE);
    public static final Namespace PQ_NS = new EntityNamespace("pq", PQ_NAMESPACE);
    public static final Namespace PQV_NS = new EntityNamespace("pqv", PQV_NAMESPACE);
    public static final Namespace PQN_NS = new EntityNamespace("pqn", PQN_NAMESPACE);
    public static final Namespace PR_NS = new EntityNamespace("pr", PR_NAMESPACE);
    public static final Namespace PRV_NS = new EntityNamespace("prv", PRV_NAMESPACE);
    public static final Namespace PRN_NS = new EntityNamespace("prn", PRN_NAMESPACE);

    // common ID namespaces
    public static final Namespace ISBN_NS = new SimpleNamespace("isbn", "urn:ISBN:");
    public static final Namespace ORCID_NS = new OrcidNamespace("orcid", "https://orcid.org/");
    public static final Namespace GKG_NS = new SimpleNamespace("gkg", "http://g.co/kg/g/");
    public static final Namespace FREEBASE_NS = new SimpleNamespace("freebase", "http://g.co/kg/m/");
    public static final Namespace GEONAMES_NS = new SimpleNamespace("geonames", "http://sws.geonames.org/");
    public static final Namespace ENTREZ_NS = new IntegerNamespace("entrez", "http://purl.uniprot.org/geneid/");
    public static final Namespace UNIPROT_NS = new SimpleNamespace("uniprot", "http://purl.uniprot.org/uniprot/");
    public static final Namespace GND_NS = new SimpleNamespace("gnd", "https://d-nb.info/gnd/");
    public static final Namespace LOC_NS = new SimpleNamespace("loc", "http://id.loc.gov/authorities/names/");
    public static final Namespace VIAF_NS = new IntegerNamespace("viaf", "http://viaf.org/viaf/");
    public static final Namespace MAG_NS = new IntegerNamespace("mag", "http://ma-graph.org/entity/");
    public static final Namespace MESH_NS = new SimpleNamespace("mesh", "http://id.nlm.nih.gov/mesh/");
    public static final Namespace EUNIS_NS = new IntegerNamespace("eunis", "http://eunis.eea.europa.eu/species/");
    public static final Namespace BABELNET_NS = new SimpleNamespace("babel", "http://babelnet.org/rdf/");
    public static final Namespace OS_NS = new IntegerNamespace("os", "http://data.ordnancesurvey.co.uk/id/");
    public static final Namespace PUBCHEM_CID_NS = new PrefixedIntegerNamespace("pubchem_cid", "http://rdf.ncbi.nlm.nih.gov/pubchem/compound/", "CID");
    public static final Namespace PUBCHEM_SID_NS = new PrefixedIntegerNamespace("pubchem_sid", "http://rdf.ncbi.nlm.nih.gov/pubchem/substance/", "SID");
    public static final Namespace MUSICBRAINZ_AREA_NS = new UUIDNamespace("mb_area", "http://musicbrainz.org/area/");
    public static final Namespace MUSICBRAINZ_ARTIST_NS = new UUIDNamespace("mb_artist", "http://musicbrainz.org/artist/");
    public static final Namespace MUSICBRAINZ_EVENT_NS = new UUIDNamespace("mb_event", "http://musicbrainz.org/event/");
    public static final Namespace MUSICBRAINZ_INSTRUMENT_NS = new UUIDNamespace("mb_instrument", "http://musicbrainz.org/instrument/");
    public static final Namespace MUSICBRAINZ_PLACE_NS = new UUIDNamespace("mb_place", "http://musicbrainz.org/place/");
    public static final Namespace MUSICBRAINZ_RECORDING_NS = new UUIDNamespace("mb_recording", "http://musicbrainz.org/recording/");
    public static final Namespace MUSICBRAINZ_RELEASE_GROUP_NS = new UUIDNamespace("mb_release_group", "http://musicbrainz.org/release-group/");
    public static final Namespace MUSICBRAINZ_RELEASE_NS = new UUIDNamespace("mb_release", "http://musicbrainz.org/release/");
    public static final Namespace MUSICBRAINZ_SERIES_NS = new UUIDNamespace("mb_series", "http://musicbrainz.org/series/");

    public static final Namespace EN_WIKIPEDIA_NS = new SimpleNamespace("wiki_en", "https://en.wikipedia.org/wiki/");
    public static final Namespace CEB_WIKIPEDIA_NS = new SimpleNamespace("wiki_ceb", "https://ceb.wikipedia.org/wiki/");
    public static final Namespace SV_WIKIPEDIA_NS = new SimpleNamespace("wiki_sv", "https://sv.wikipedia.org/wiki/");
    public static final Namespace FR_WIKIPEDIA_NS = new SimpleNamespace("wiki_fr", "https://fr.wikipedia.org/wiki/");
    public static final Namespace DE_WIKIPEDIA_NS = new SimpleNamespace("wiki_de", "https://de.wikipedia.org/wiki/");
    public static final Namespace ES_WIKIPEDIA_NS = new SimpleNamespace("wiki_es", "https://es.wikipedia.org/wiki/");
    public static final Namespace IT_WIKIPEDIA_NS = new SimpleNamespace("wiki_it", "https://it.wikipedia.org/wiki/");
    public static final Namespace WIKIMEDIA_NS = new SimpleNamespace("wikimedia", "https://commons.wikimedia.org/wiki/");

    // popular items
    public static final IRI HUMAN = SVF.createIRI(WD_NAMESPACE, "Q5");
    public static final IRI UNITED_STATES = SVF.createIRI(WD_NAMESPACE, "Q30");
    public static final IRI ENGLISH_WIKIPEDIA = SVF.createIRI(WD_NAMESPACE, "Q328");
    public static final IRI ENGLISH = SVF.createIRI(WD_NAMESPACE, "Q1860");
    public static final IRI SPECIES = SVF.createIRI(WD_NAMESPACE, "Q7432");
    public static final IRI GENE = SVF.createIRI(WD_NAMESPACE, "Q7187");
    public static final IRI METRE = SVF.createIRI(WD_NAMESPACE, "Q11573");
    public static final IRI TAXON = SVF.createIRI(WD_NAMESPACE, "Q16521");
    public static final IRI PUBMED_CENTRAL = SVF.createIRI(WD_NAMESPACE, "Q229883");
    public static final IRI TITLE = SVF.createIRI(WD_NAMESPACE, "Q783521");
    public static final IRI WIKIMEDIA_CATEGORY = SVF.createIRI(WD_NAMESPACE, "Q4167836");
    public static final IRI CROSSREF = SVF.createIRI(WD_NAMESPACE, "Q5188229");
    public static final IRI EUROPE_PUBMED_CENTRAL = SVF.createIRI(WD_NAMESPACE, "Q5412157");
    public static final IRI GBIF = SVF.createIRI(WD_NAMESPACE, "Q1531570");
    public static final IRI MALE = SVF.createIRI(WD_NAMESPACE, "Q6581097");
    public static final IRI FEMALE = SVF.createIRI(WD_NAMESPACE, "Q6581072");
    public static final IRI SCHOLARLY_ARTICLE = SVF.createIRI(WD_NAMESPACE, "Q13442814");
    public static final IRI FREEBASE = SVF.createIRI(WD_NAMESPACE, "Q15241312");

    public static final IRI EN_WIKIPEDIA = SVF.createIRI("https://en.wikipedia.org/");
    public static final IRI CEB_WIKIPEDIA = SVF.createIRI("https://ceb.wikipedia.org/");
    public static final IRI SV_WIKIPEDIA = SVF.createIRI("https://sv.wikipedia.org/");
    public static final IRI FR_WIKIPEDIA = SVF.createIRI("https://fr.wikipedia.org/");
    public static final IRI DE_WIKIPEDIA = SVF.createIRI("https://de.wikipedia.org/");
    public static final IRI ES_WIKIPEDIA = SVF.createIRI("https://es.wikipedia.org/");
    public static final IRI IT_WIKIPEDIA = SVF.createIRI("https://it.wikipedia.org/");
    public static final IRI WIKIMEDIA = SVF.createIRI("https://commons.wikimedia.org/");

    public static final class Properties {
        // popular properties
        public static final String COUNTRY = "P17";
        public static final String IMAGE = "P18";
        public static final String PLACE_OF_BIRTH = "P19";
        public static final String PLACE_OF_DEATH = "P20";
        public static final String SEX_OR_GENDER = "P21";
        public static final String INSTANCE_OF = "P31";
        public static final String AUTHOR = "P50";
        public static final String FAMILY = "P53";
        public static final String EDUCATED_AT = "P69";
        public static final String OCCUPATION = "P106";
        public static final String EMPLOYER = "P108";
        public static final String IMPORTED_FROM_WIKIMEDIA_PROJECT = "P143";
        public static final String FOLLOWS = "P155";
        public static final String FOLLOWED_BY= "P156";
        public static final String VIAF_ID = "P214";
        public static final String GND_ID = "P227";
        public static final String STATED_IN = "P248";
        public static final String LOCATION = "P276";
        public static final String SUBCLASS_OF = "P279";
        public static final String POSTAL_CODE = "P281";
        public static final String PAGES = "P304";
        public static final String ENTREZ_GENE_ID = "P351";
        public static final String UNIPROT_PROTEIN_ID = "P352";
        public static final String DOI = "P356";
        public static final String PART_OF = "P361";
        public static final String ISSUE = "P433";
        public static final String VOLUME = "P478";
        public static final String ORCID = "P496";
        public static final String CATALOG_CODE = "P528";
        public static final String DATE_OF_BIRTH = "P569";
        public static final String DATE_OF_DEATH = "P570";
        public static final String INCEPTION = "P571";
        public static final String PUBLICATION_DATE = "P577";
        public static final String START_TIME = "P580";
        public static final String END_TIME = "P582";
        public static final String POINT_IN_TIME = "P585";
        public static final String COORDINATE_LOCATION = "P625";
        public static final String OF = "P642";
        public static final String FREEBASE_ID = "P646";
        public static final String STREET_NUMBER = "P670";
        public static final String ORTHOLOG = "P684";
        public static final String PUBMED_ID = "P698";
        public static final String FAMILY_NAME = "P734";
        public static final String GIVEN_NAME = "P735";
        public static final String RETRIEVED = "P813";
        public static final String ARXIV_ID = "P818";
        public static final String REFERENCE_URL = "P854";
        public static final String OFFICIAL_WEBSITE = "P856";
        public static final String MAIN_SUBJECT = "P921";
        public static final String PMCID = "P932";
        public static final String WORK_LOCATION = "P937";
        public static final String CATALOG = "P972";
        public static final String CHROMOSOME = "P1057";
        public static final String POPULATION = "P1082";
        public static final String APPARENT_MAGNITUDE = "P1215";
        public static final String ASTRONOMICAL_FILTER = "P1227";
        public static final String PUBLISHED_IN = "P1433";
        public static final String OFFICIAL_NAME = "P1448";
        public static final String TITLE = "P1476";
        public static final String SERIES_ORDINAL = "P1545";
        public static final String GEONAMES_ID = "P1566";
        public static final String SEE_ALSO = "P1659";
        public static final String NATIVE_LABEL = "P1705";
        public static final String NAMED_AS = "P1810";
        public static final String SHORT_NAME = "P1813";
        public static final String LENGTH = "P2043";
        public static final String AREA = "P2046";
        public static final String DURATION = "P2047";
        public static final String HEIGHT = "P2048";
        public static final String WIDTH = "P2049";
        public static final String AUTHOR_NAME_STRING = "P2093";
        public static final String PARALLAX = "P2214";
        public static final String PROPER_MOTION = "P2215";
        public static final String RADIAL_VELOCITY = "P2216";
        public static final String BABELNET_ID = "P2581";
        public static final String GOOGLE_KNOWLEDGE_GRAPH_ID = "P2671";
        public static final String CITES_WORK = "P2860";
        public static final String EXACT_MATCH = "P2888";
        public static final String SIMBAD_ID = "P3083";
        public static final String WIKIMEDIA_IMPORT_URL = "P4656";
        public static final String EUNIS_ID = "P6177";
        public static final String RIGHT_ASCENSION = "P6257";
        public static final String DECLINATION = "P6258";
        public static final String EPOCH = "P6259";
        public static final String STREET_ADDRESS = "P6375";
    }

	public static Collection<IRI> getIRIs() {
		List<String> properties = new ArrayList<>(61);
		for (Field f : Properties.class.getFields()) {
			if (f.getType() == Namespace.class) {
				try {
					properties.add((String) f.get(null));
				} catch (IllegalAccessException ex) {
					throw new AssertionError(ex);
				}
			}
		}
		String[] propertyNamespaces = {
			WD_NAMESPACE, WDT_NAMESPACE, WDTN_NAMESPACE, WDNO_NAMESPACE,
			P_NAMESPACE, PS_NAMESPACE, PSV_NAMESPACE, PSN_NAMESPACE,
			PQ_NAMESPACE, PQV_NAMESPACE, PQN_NAMESPACE, PR_NAMESPACE, PRV_NAMESPACE, PRN_NAMESPACE
		};

		List<IRI> iris = new ArrayList<>(properties.size()*propertyNamespaces.length);
		for (String ns : propertyNamespaces) {
			for (String prop : properties) {
				iris.add(SVF.createIRI(ns, prop));
			}
		}
		return iris;
	}

	static final class EntityNamespace extends AbstractIRIEncodingNamespace {
		private static final long serialVersionUID = 1637550316595185157L;

		EntityNamespace(String prefix, String ns) {
			super(prefix, ns);
		}

		@Override
		public ByteBuffer writeBytes(String localName, ByteBuffer b) {
			int pos = localName.indexOf('-');
			if (pos != -1) {
				int len = localName.length() - pos - 1;
				b = ValueIO.ensureCapacity(b, 2 + len);
				b.put((byte) '-');
				byte[] subId = Bytes.toBytes(localName.substring(pos+1));
				b.put((byte)subId.length);
				b.put(subId);
			}
			b = ValueIO.ensureCapacity(b, 1);
			b.put((byte) localName.charAt(0));
			return ValueIO.writeCompressedInteger(localName.substring(1, pos != -1 ? pos : localName.length()), b);
		}

		@Override
		public String readBytes(ByteBuffer b) {
			String suffix;
			char type = (char) b.get();
			if (type == '-') {
				int len = b.get();
				byte[] subId = new byte[len];
				b.get(subId);
				suffix = "-" + Bytes.toString(subId);
				type = (char) b.get();
			} else {
				suffix = "";
			}
			String id = ValueIO.readCompressedInteger(b);
			return type + id + suffix;
		}
	}

	static final class HashNamespace extends AbstractIRIEncodingNamespace {
		private static final long serialVersionUID = 2218386262774783440L;

		HashNamespace(String prefix, String ns) {
			super(prefix, ns);
		}

		@Override
		public ByteBuffer writeBytes(String localName, ByteBuffer b) {
			byte[] hexBytes = Bytes.fromHex(localName);
			b = ValueIO.ensureCapacity(b, hexBytes.length);
			b.put(hexBytes);
			return b;
		}

		@Override
		public String readBytes(ByteBuffer b) {
			byte[] hexBytes = new byte[b.remaining()];
			b.get(hexBytes);
			return Bytes.toHex(hexBytes);
		}
	}

	static final class StatementNamespace extends AbstractIRIEncodingNamespace {
		private static final long serialVersionUID = -3329839557078371520L;

		public StatementNamespace(String prefix, String name) {
			super(prefix, name);
		}

		@Override
		public ByteBuffer writeBytes(String localName, ByteBuffer b) {
			int uuidPos = localName.indexOf('-');
			UUID uuid = UUID.fromString(localName.substring(uuidPos+1));
			b = ValueIO.ensureCapacity(b, 1 + Long.BYTES + Long.BYTES);
			b.put((byte) localName.charAt(0));
			b.putLong(uuid.getMostSignificantBits());
			b.putLong(uuid.getLeastSignificantBits());
			return ValueIO.writeCompressedInteger(localName.substring(1, uuidPos), b);
		}

		@Override
		public String readBytes(ByteBuffer b) {
			char type = (char) b.get();
			long uuidMost = b.getLong();
			long uuidLeast = b.getLong();
			String id = ValueIO.readCompressedInteger(b);
			UUID uuid = new UUID(uuidMost, uuidLeast);
			return type + id + "-" + uuid.toString();
		}
	}

	static final class OrcidNamespace extends AbstractIRIEncodingNamespace {
		private static final long serialVersionUID = -8193568694174435059L;

		public OrcidNamespace(String prefix, String name) {
			super(prefix, name);
		}

		@Override
		public ByteBuffer writeBytes(String localName, ByteBuffer b) {
			if (localName.length() != 19) {
				throw new IllegalArgumentException(String.format("Invalid length for ORCID: %s", localName));
			}
			int checksumPos = localName.length()-1;
			char checksum = localName.charAt(checksumPos);
			// prefix with 1 to maintain leading zeros
			BigInteger id = new BigInteger("1"+localName.substring(0, checksumPos).replace("-", ""));
			byte[] bytes = id.toByteArray();
			b = ValueIO.ensureCapacity(b, 1+bytes.length+1);
			return b.put((byte) bytes.length).put(bytes).put((byte)checksum);
		}

		@Override
		public String readBytes(ByteBuffer b) {
			int len = b.get();
			byte[] idBytes = new byte[len];
			b.get(idBytes);
			char checksum = (char) b.get();
			BigInteger id = new BigInteger(idBytes);
			String idStr = id.toString();
			return idStr.substring(1, 5) + "-" + idStr.substring(5, 9) + "-" + idStr.substring(9, 13) + "-" + idStr.substring(13) + checksum;
		}
	}
}
