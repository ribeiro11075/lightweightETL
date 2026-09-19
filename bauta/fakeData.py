"""The lists the `fake*` masking strategies pick from: names, cities, streets
and companies, by locale.

Data only; builtinMasking's fake* strategies use it, and the native masker is
handed these same lists, so this is the one copy of them. Changing an entry
changes every mask already made from its list -- mask-rs/vectors/reference.json
records them all, in every locale, so a change is deliberate.
"""
from __future__ import annotations

from typing import Dict, NamedTuple, Tuple


FIRST_NAMES = (
    'Ada', 'Alan', 'Alice', 'Amara', 'Andre', 'Anya', 'Arjun', 'Beatriz', 'Bruno', 'Camila', 'Carlos', 'Chen', 'Chloe', 'Dara', 'David',
    'Diego', 'Elena', 'Elif', 'Emeka', 'Emma', 'Ethan', 'Fatima', 'Felix', 'Freya', 'Grace', 'Hana', 'Hugo', 'Ines', 'Isaac', 'Ivan',
    'Jada', 'James', 'Jin', 'Jonas', 'Kai', 'Kofi', 'Lara', 'Leo', 'Lina', 'Lucas', 'Maya', 'Mateo', 'Mei', 'Mila', 'Nadia', 'Noah',
    'Nora', 'Omar', 'Oscar', 'Priya', 'Quinn', 'Rafael', 'Rosa', 'Sami', 'Sara', 'Theo', 'Uma', 'Victor', 'Wren', 'Yara', 'Yusuf', 'Zoe',
    )

LAST_NAMES = (
    'Abara', 'Alvarez', 'Anand', 'Bauer', 'Becker', 'Bianchi', 'Brooks', 'Castillo', 'Chandra', 'Costa', 'Dahl', 'Diallo', 'Dubois',
    'Eriksen', 'Ferreira', 'Fischer', 'Garcia', 'Haddad', 'Hansen', 'Hayes', 'Ibrahim', 'Ito', 'Jensen', 'Kaplan', 'Kim', 'Kowalski',
    'Larsen', 'Lopes', 'Mendes', 'Moreau', 'Murphy', 'Nakamura', 'Novak', 'Okafor', 'Olsen', 'Park', 'Patel', 'Perez', 'Quinlan',
    'Reyes', 'Rossi', 'Santos', 'Schmidt', 'Silva', 'Singh', 'Sousa', 'Tanaka', 'Torres', 'Vargas', 'Varga', 'Wagner', 'Walsh',
    'Weber', 'Wong', 'Yamada', 'Young', 'Zhang', 'Ziegler', 'Adeyemi', 'Bergstrom', 'Carvalho', 'Duarte', 'Falk', 'Lindqvist',
    )

CITIES = (
    'Ashford', 'Bayview', 'Brookfield', 'Cedar Falls', 'Clearwater', 'Crestwood', 'Eastport', 'Elmstead', 'Fairhaven', 'Glenmoor',
    'Greystone', 'Hartwell', 'Highbridge', 'Kingsley', 'Lakemont', 'Maple Grove', 'Marlow', 'Millbrook', 'Northgate', 'Oakridge',
    'Pinecrest', 'Port Albany', 'Ravenswood', 'Redcliff', 'Riverton', 'Rosedale', 'Sandhurst', 'Silverton', 'Southwick', 'Stonebridge',
    'Thornbury', 'Westbrook',
    )

COMPANY_WORDS = (
    'Acorn', 'Apex', 'Beacon', 'Blue Harbor', 'Brightline', 'Cobalt', 'Copperleaf', 'Evergreen', 'Fieldstone', 'Granite', 'Harborview',
    'Ironwood', 'Juniper', 'Keystone', 'Lighthouse', 'Meridian', 'Northwind', 'Oakline', 'Pioneer', 'Quarry', 'Redwood', 'Summit',
    'Tidewater', 'Vantage',
    )

COMPANY_SUFFIXES = ('Analytics', 'Group', 'Holdings', 'Industries', 'Labs', 'Logistics', 'Partners', 'Systems')

STREET_NAMES = (
    'Ash', 'Birch', 'Bridge', 'Canal', 'Cedar', 'Chapel', 'Church', 'Elm', 'Forest', 'Garden', 'Hill', 'Lake', 'Maple', 'Meadow',
    'Mill', 'Oak', 'Orchard', 'Park', 'Pine', 'River', 'School', 'Spring', 'Station', 'Willow',
    )

STREET_SUFFIXES = ('Street', 'Avenue', 'Road', 'Lane', 'Way', 'Drive', 'Court', 'Place')


class Locale(NamedTuple):
    """Names, places and address layout for one country's fake data.

    `address` is a format taking `number`, `street` (from `streets`) and `kind`
    (from `streetKinds`) -- the part that varies most between countries.
    """

    firstNames: Tuple[str, ...]
    lastNames: Tuple[str, ...]
    cities: Tuple[str, ...]
    streets: Tuple[str, ...]
    streetKinds: Tuple[str, ...]
    address: str
    companySuffixes: Tuple[str, ...]


def _words(text: str) -> Tuple[str, ...]:

    return tuple(word.strip() for word in text.split(',') if word.strip())


LOCALES: Dict[str, Locale] = {
    'en_US': Locale(
        _words('James, Mary, Robert, Patricia, John, Jennifer, Michael, Linda, David, Elizabeth, William, Barbara, Richard, Susan, '
               'Joseph, Jessica, Thomas, Sarah, Charles, Karen, Christopher, Lisa, Daniel, Nancy, Matthew, Betty, Anthony, Sandra'),
        _words('Smith, Johnson, Williams, Brown, Jones, Garcia, Miller, Davis, Rodriguez, Martinez, Hernandez, Lopez, Gonzalez, '
               'Wilson, Anderson, Thomas, Taylor, Moore, Jackson, Martin, Lee, Perez, Thompson, White, Harris, Sanchez, Clark, Lewis'),
        _words('Springfield, Riverside, Franklin, Greenville, Clinton, Fairview, Salem, Madison, Georgetown, Arlington, Ashland, '
               'Burlington, Manchester, Oxford, Milton, Clayton, Dayton, Lexington, Milford, Bristol'),
        _words('Main, Oak, Pine, Maple, Cedar, Elm, Washington, Lake, Hill, Park, Walnut, Spring'),
        _words('Street, Avenue, Road, Drive, Lane, Court, Boulevard, Way'),
        '{number} {street} {kind}', _words('Inc., LLC, Corp., Co.')),
    'en_GB': Locale(
        _words('Oliver, Amelia, George, Isla, Harry, Ava, Jack, Mia, Jacob, Emily, Charlie, Sophie, Thomas, Grace, Oscar, Lily, '
               'William, Freya, James, Evie, Alfie, Ella, Henry, Poppy'),
        _words('Smith, Jones, Taylor, Brown, Williams, Wilson, Johnson, Davies, Robinson, Wright, Thompson, Evans, Walker, White, '
               'Roberts, Green, Hall, Wood, Jackson, Clarke, Hughes, Edwards, Turner, Hill'),
        _words('Bradford, Chester, Durham, Exeter, Harrogate, Kendal, Lincoln, Ludlow, Norwich, Reading, Salisbury, Stafford, Truro, '
               'Wells, Whitby, Winchester, Worcester, York, Bath, Carlisle'),
        _words('High, Church, Station, Victoria, Park, Mill, Queen, King, School, London, Manor, Chapel'),
        _words('Street, Road, Lane, Close, Avenue, Way, Gardens, Crescent'),
        '{number} {street} {kind}', _words('Ltd, PLC, LLP')),
    'de_DE': Locale(
        _words('Lukas, Anna, Leon, Mia, Finn, Emma, Jonas, Hannah, Paul, Lea, Felix, Lena, Maximilian, Marie, Elias, Sophie, Noah, '
               'Laura, Ben, Julia, Tim, Lisa, Jan, Katharina'),
        _words('Müller, Schmidt, Schneider, Fischer, Weber, Meyer, Wagner, Becker, Schulz, Hoffmann, Schäfer, Koch, Bauer, Richter, '
               'Klein, Wolf, Schröder, Neumann, Schwarz, Zimmermann, Braun, Krüger, Hofmann, Hartmann'),
        _words('Aachen, Bamberg, Bielefeld, Bonn, Celle, Darmstadt, Erfurt, Freiburg, Göttingen, Heidelberg, Kassel, Kiel, Konstanz, '
               'Lübeck, Mainz, Münster, Passau, Regensburg, Trier, Ulm'),
        _words('Haupt, Bahnhof, Garten, Schul, Kirch, Linden, Berg, Wald, Mühlen, Dorf, Birken, Rosen'),
        _words('straße, weg, gasse, allee, ring, platz'),
        '{street}{kind} {number}', _words('GmbH, AG, KG, GmbH & Co. KG')),
    'fr_FR': Locale(
        _words('Gabriel, Emma, Léo, Jade, Raphaël, Louise, Arthur, Alice, Louis, Chloé, Lucas, Lina, Adam, Rose, Jules, Léa, Hugo, '
               'Anna, Maël, Mila, Nathan, Julia, Paul, Inès'),
        _words('Martin, Bernard, Dubois, Thomas, Robert, Richard, Petit, Durand, Leroy, Moreau, Simon, Laurent, Lefebvre, Michel, '
               'Garcia, David, Bertrand, Roux, Vincent, Fournier, Morel, Girard, André, Mercier'),
        _words('Amiens, Angers, Annecy, Avignon, Besançon, Brest, Caen, Colmar, Dijon, Grenoble, Limoges, Metz, Nancy, Nîmes, '
               'Orléans, Pau, Poitiers, Reims, Rouen, Tours'),
        _words("de la Paix, des Lilas, Victor Hugo, de la Gare, du Moulin, des Écoles, de l'Église, Pasteur, Jean Jaurès, "
               'du Château, des Tilleuls, de la République'),
        _words('rue, avenue, boulevard, place, allée, chemin'),
        '{number} {kind} {street}', _words('SARL, SAS, SA, EURL')),
    'es_ES': Locale(
        _words('Hugo, Lucía, Martín, Sofía, Daniel, Martina, Pablo, María, Alejandro, Julia, Lucas, Paula, Álvaro, Valeria, Adrián, '
               'Emma, Mateo, Daniela, David, Carla, Diego, Alba, Javier, Noa'),
        _words('García, Rodríguez, González, Fernández, López, Martínez, Sánchez, Pérez, Gómez, Martín, Jiménez, Ruiz, Hernández, '
               'Díaz, Moreno, Muñoz, Álvarez, Romero, Alonso, Gutiérrez, Navarro, Torres, Domínguez, Vázquez'),
        _words('Albacete, Alicante, Badajoz, Burgos, Cáceres, Cádiz, Córdoba, Gijón, Girona, Granada, Huelva, León, Logroño, Lugo, '
               'Oviedo, Salamanca, Santander, Segovia, Toledo, Zamora'),
        _words('Mayor, Real, del Sol, de la Paz, Nueva, del Carmen, San Juan, de la Iglesia, del Mar, de Cervantes, Colón, de Goya'),
        _words('Calle, Avenida, Plaza, Paseo, Camino, Ronda'),
        '{kind} {street}, {number}', _words('S.L., S.A., S.L.U.')),
    'pt_BR': Locale(
        _words('Miguel, Helena, Arthur, Alice, Gael, Laura, Heitor, Maria, Theo, Valentina, Davi, Heloísa, Gabriel, Sophia, Bernardo, '
               'Manuela, Samuel, Júlia, João, Isabela, Pedro, Lívia, Lucas, Beatriz'),
        _words('Silva, Santos, Oliveira, Souza, Rodrigues, Ferreira, Alves, Pereira, Lima, Gomes, Costa, Ribeiro, Martins, Carvalho, '
               'Almeida, Lopes, Soares, Fernandes, Vieira, Barbosa, Rocha, Dias, Nascimento, Andrade'),
        _words('Aracaju, Belém, Blumenau, Campinas, Cuiabá, Curitiba, Florianópolis, Goiânia, Joinville, Londrina, Maceió, Manaus, '
               'Natal, Niterói, Olinda, Petrópolis, Santos, Sorocaba, Uberlândia, Vitória'),
        _words('das Flores, São João, Sete de Setembro, XV de Novembro, das Palmeiras, Santa Luzia, do Comércio, Brasil, da Paz, '
               'Dom Pedro II, das Acácias, Tiradentes'),
        _words('Rua, Avenida, Travessa, Praça, Alameda, Estrada'),
        '{kind} {street}, {number}', _words('Ltda., S.A., ME')),
    'it_IT': Locale(
        _words('Leonardo, Sofia, Francesco, Aurora, Tommaso, Giulia, Edoardo, Ginevra, Alessandro, Beatrice, Lorenzo, Alice, Mattia, '
               'Vittoria, Gabriele, Emma, Riccardo, Ludovica, Andrea, Matilde, Diego, Chiara, Nicolò, Anna'),
        _words('Rossi, Russo, Ferrari, Esposito, Bianchi, Romano, Colombo, Ricci, Marino, Greco, Bruno, Gallo, Conti, De Luca, '
               'Mancini, Costa, Giordano, Rizzo, Lombardi, Moretti, Barbieri, Fontana, Santoro, Mariani'),
        _words('Ancona, Arezzo, Bergamo, Bologna, Brescia, Cagliari, Como, Cremona, Ferrara, Lecce, Lucca, Mantova, Modena, Padova, '
               'Parma, Perugia, Pisa, Ravenna, Siena, Trento'),
        _words('Roma, Garibaldi, Mazzini, Dante, Verdi, Cavour, Marconi, dei Mille, della Libertà, Vittorio Emanuele, San Francesco, '
               'del Popolo'),
        _words('Via, Viale, Piazza, Corso, Vicolo, Largo'),
        '{kind} {street} {number}', _words('S.r.l., S.p.A., S.a.s., S.n.c.')),
    'nl_NL': Locale(
        _words('Noah, Emma, Luca, Julia, Sem, Mila, Lucas, Tess, Levi, Sophie, Finn, Zoë, Daan, Sara, Milan, Nora, Bram, Yara, Mees, '
               'Eva, Jesse, Liv, Thijs, Anna'),
        _words('de Jong, Jansen, de Vries, van den Berg, van Dijk, Bakker, Janssen, Visser, Smit, Meijer, de Boer, Mulder, de Groot, '
               'Bos, Vos, Peters, Hendriks, van Leeuwen, Dekker, Brouwer, de Wit, Dijkstra, Smits, de Graaf'),
        _words('Alkmaar, Amersfoort, Apeldoorn, Arnhem, Breda, Delft, Deventer, Dordrecht, Enschede, Gouda, Groningen, Haarlem, '
               'Leeuwarden, Leiden, Maastricht, Nijmegen, Tilburg, Utrecht, Zwolle, Zaandam'),
        _words('Kerk, Molen, School, Dorps, Linden, Beuken, Stations, Wilhelmina, Juliana, Nieuwe, Oranje, Eiken'),
        _words('straat, weg, laan, plein, singel, gracht'),
        '{street}{kind} {number}', _words('B.V., N.V., V.O.F.')),
    }

# The lists used without a `locale` option: a deliberately international mix.
# Kept exactly as they were, since changing them would change every mask
# already written with them.
DEFAULT_LOCALE = Locale(FIRST_NAMES, LAST_NAMES, CITIES, STREET_NAMES, STREET_SUFFIXES, '{number} {street} {kind}', COMPANY_SUFFIXES)
