from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote, urlparse

from location_data import CITIES


class GooglePlacesAPIError(Exception):
    pass


NICHES = [
    "Physical therapists",
    "Dentists",
    "Auto repair shops",
    "Moving companies",
    "Cleaning companies",
    "Beauty & wellness (premium)",
    "Real estate agents",
    "Lawyers / tax advisors",
    "Pet services",
    "Plumbing & heating",
    "Gardening & landscaping",
    "Hair salons",
    "Restaurants",
    "Cafes",
    "Hotels",
    "Gyms / fitness",
    "Electricians",
    "Roofing",
    "Locksmiths",
    "HVAC",
    "Car wash",
    "Florists",
    "Bakeries",
    "Pharmacies",
    "Optometrists",
    "Insurance agents",
    "Accountants",
    "Photographers",
    "Dry cleaning",
    "IT support",
    "Marketing agencies",
    "Catering",
    "Web design agencies",
    "Software development companies",
    "Managed IT services",
    "Cybersecurity services",
    "Business consultants",
    "Coworking spaces",
    "Translation services",
    "Funeral services",
    "Tattoo studios",
    "Nail salons",
    "Barbershops",
    "Massage therapists",
    "Podiatrists",
    "Veterinary clinics",
    "Dental labs",
    "Orthodontists",
    "Private schools",
    "Language schools",
    "Driving schools",
    "Childcare centers",
    "Event venues",
    "Wedding planners",
    "Home cleaning services",
    "Solar installers",
    "Painters",
    "Handyman services",
    "Kitchen studios",
    "Furniture stores",
    "Architects",
    "Interior designers",
    "General contractors",
    "Security system installers",
    "Signage companies",
    "Print shops",
    "Copy shops",
    "Recruitment agencies",
    "HR consultants",
    "Payroll services",
    "E-commerce agencies",
    "Video production companies",
    "PR agencies",
    "B2B sales consultants",
    "Logistics companies",
    "Courier services",
    "Storage facilities",
    "Medical clinics",
    "Dermatologists",
    "Chiropractors",
    "Speech therapists",
    "Occupational therapists",
    "Orthopedic clinics",
    "Psychologists",
    "Denturists",
    "Car dealerships",
    "Tire shops",
    "Window cleaning services",
    "Pest control services",
    "Security guard services",
    "Yoga studios",
    "Pilates studios",
    "CrossFit gyms",
    "Martial arts schools",
    "Dance studios",
    "Music schools",
    "Art schools",
    "Bookstores",
    "Electronics stores",
    "Hardware stores",
    "Garden centers",
    "Pet stores",
    "Toy stores",
    "Shoe stores",
    "Jewelry stores",
    "Phone repair shops",
    "Computer repair shops",
    "Appliance repair services",
    "Bicycle shops",
    "Motorcycle dealerships",
    "Gas stations",
    "Laundromats",
    "Tailors",
    "Party supply stores",
    "Office supply stores",
    "Home health care services",
    "Urgent care centers",
    "Walk-in clinics",
    "Medical laboratories",
    "Audiologists",
    "Hearing aid centers",
    "Ophthalmologists",
    "Cardiologists",
    "Family doctors / GPs",
    "Pediatricians",
    "Psychiatrists",
    "Nutritionists",
    "Acupuncture clinics",
    "Medical spas",
    "Day spas",
    "Breweries",
    "Bars & pubs",
    "Nightclubs",
    "Food trucks",
    "Bowling alleys",
    "Golf courses",
    "Escape rooms",
    "Movie theaters",
    "Swimming schools",
    "Tennis clubs",
    "Metal fabrication shops",
    "Welding services",
    "CNC machining shops",
    "3D printing services",
    "Packaging companies",
    "Shipping centers",
    "Freight forwarders",
    "Waste management companies",
    "Recycling centers",
    "Auto body shops",
    "Auto glass repair",
    "RV parks",
    "Campgrounds",
    "Bed and breakfasts",
    "Hostels",
    "Vacation rental managers",
    "Property management companies",
    "Land surveyors",
    "Environmental consultants",
    "Patent attorneys",
    "Immigration lawyers",
    "Notaries",
    "Mediation services",
    "Smoke shops",
    "Vape shops",
    "Farm equipment dealers",
    "Aquariums & pet fish stores",
    "Sports equipment stores",
    "Musical instrument stores",
    "Antique stores",
    "Thrift stores",
    "Consignment shops",
    "Pawn shops",
    "Gun stores & shooting ranges",
    "Hunting & outdoor stores",
    "Camping gear stores",
    "Ski & snowboard shops",
    "Surf shops",
    "Scuba dive shops",
    "Boat repair services",
    "Marinas",
    "Flight schools",
    "Trucking companies",
    "Taxi services",
    "Limousine services",
    "Charter bus services",
    "Airport shuttle services",
    "Parking lot operators",
    "EV charging installers",
    "Battery stores",
    "Lighting stores",
    "Flooring stores",
    "Carpet stores",
    "Tile & stone showrooms",
    "Window treatment companies",
    "Blinds & shutters installers",
    "Pool builders",
    "Pool cleaning services",
    "Hot tub dealers",
    "Sauna installers",
    "Fence contractors",
    "Deck builders",
    "Paving contractors",
    "Asphalt contractors",
    "Concrete contractors",
    "Masonry contractors",
    "Demolition contractors",
    "Excavation contractors",
    "Crane rental services",
    "Scaffolding companies",
    "Construction equipment rental",
    "Tool rental shops",
    "Party equipment rental",
    "Tent rental services",
    "AV equipment rental",
    "Stage & event rental",
    "Uniform suppliers",
    "Workwear stores",
    "Safety equipment suppliers",
    "Industrial supply stores",
    "Laboratory supply companies",
    "Dental supply companies",
    "Veterinary supply companies",
    "Restaurant supply stores",
    "Commercial kitchen equipment",
    "Coffee roasteries",
    "Tea shops",
    "Juice bars",
    "Smoothie shops",
    "Ice cream shops",
    "Gelato shops",
    "Chocolate shops",
    "Candy stores",
    "Butcher shops",
    "Fish markets",
    "Organic grocery stores",
    "Health food stores",
    "Vitamin & supplement stores",
    "CBD shops",
    "Costume stores",
    "Bridal shops",
    "Formal wear rental",
    "Luggage stores",
    "Travel agencies",
    "Tour operators",
    "Cruise travel agencies",
    "Visa & passport services",
    "Pack & ship centers",
    "Mailbox & shipping stores",
    "Check cashing services",
    "Currency exchange services",
    "Bail bond agencies",
    "Private investigators",
    "Security consulting firms",
    "Fire protection services",
    "Fire sprinkler contractors",
    "Elevator service companies",
    "Building inspection services",
    "Home inspectors",
    "Energy auditors",
    "Insulation contractors",
    "Waterproofing companies",
    "Foundation repair companies",
    "Septic tank services",
    "Chimney sweeps",
    "Gutter cleaning services",
    "Pressure washing companies",
    "Snow removal services",
    "Landscaping supply yards",
    "Tree service companies",
    "Arborists",
    "Lawn care companies",
    "Irrigation contractors",
    "Sod & turf suppliers",
    "Equestrian centers",
    "Dog trainers",
    "Dog daycare centers",
    "Pet grooming salons",
    "Pet boarding kennels",
    "Aquarium maintenance services",
    "Taxidermy studios",
    "Mini golf courses",
    "Trampoline parks",
    "Indoor playground centers",
    "Laser tag arenas",
    "Go-kart tracks",
    "Racquet sports clubs",
    "Rowing clubs",
    "Sailing schools",
    "Fishing charter services",
    "Embroidery shops",
    "Screen printing shops",
    "Trophy & awards shops",
    "Engraving services",
    "Document shredding services",
    "Records storage services",
    "Medical courier services",
    "Medical waste disposal",
    "Mold remediation services",
    "Asbestos abatement contractors",
    "Radon mitigation services",
    "Air duct cleaning services",
    "Carpet cleaning companies",
    "Upholstery cleaning services",
    "Furniture restoration shops",
    "Piano tuners",
    "Musical instrument repair shops",
    "Watch repair shops",
    "Jewelry repair services",
    "Mattress stores",
    "Bedding stores",
    "Sewing & craft stores",
    "Hobby shops",
    "Comic book stores",
    "Collectibles shops",
    "Vinyl record stores",
    "Video game stores",
    "Electronics recycling centers",
    "Mobile phone stores",
    "Home theater installers",
    "Smart home installers",
    "Satellite TV installers",
    "Internet service providers",
    "Shoe repair shops",
    "Leather repair shops",
    "Cosmetic dentists",
    "Endodontists",
    "Periodontists",
    "Prosthodontists",
    "Oral surgeons",
    "Family practice doctors",
    "Urologists",
    "Gastroenterologists",
    "Pulmonologists",
    "Endocrinologists",
    "Rheumatologists",
    "Nephrologists",
    "Allergists & immunologists",
    "Infectious disease specialists",
    "Pain management clinics",
    "Physical rehabilitation centers",
    "Sports medicine clinics",
    "Fertility clinics",
    "Sleep medicine clinics",
    "Medical imaging centers",
    "Dialysis centers",
    "Hospice care services",
    "Home care nursing agencies",
    "Medical equipment rental",
]

NICHE_SEARCH_VARIATIONS: dict[str, list[str]] = {
    "Physical therapists": ["Physical therapists", "Physiotherapeut", "Physiotherapie", "Physio"],
    "Dentists": ["Dentists", "Zahnarzt", "Zahnarztpraxis", "Zahnärzte"],
    "Auto repair shops": ["Auto repair shops", "Autowerkstatt", "Kfz-Werkstatt", "Autoreparatur"],
    "Moving companies": ["Moving companies", "Umzugsunternehmen", "Möbelspedition", "Umzug"],
    "Cleaning companies": ["Cleaning companies", "Reinigungsservice", "Gebäudereinigung", "Putzdienst"],
    "Beauty & wellness (premium)": ["Beauty & wellness", "Kosmetikstudio", "Wellness", "Schönheit"],
    "Real estate agents": ["Real estate agents", "Immobilienmakler", "Makler", "Immobilien"],
    "Lawyers / tax advisors": ["Lawyers", "Rechtsanwalt", "Steuerberater", "Anwalt"],
    "Pet services": ["Pet services", "Tierarzt", "Hundesalon", "Tierpflege"],
    "Plumbing & heating": ["Plumbing", "Klempner", "Heizung", "Sanitär"],
    "Gardening & landscaping": ["Gardening", "Gartenbau", "Landschaftsgestaltung", "Gärtner"],
    "Hair salons": ["Hair salon", "Friseur", "Friseursalon", "Haarsalon"],
    "Restaurants": ["Restaurant", "Restaurants", "Gaststätte", "Essen"],
    "Cafes": ["Cafe", "Café", "Kaffee", "Kaffeebar"],
    "Hotels": ["Hotel", "Hotels", "Unterkunft", "Pension"],
    "Gyms / fitness": ["Gym", "Fitnessstudio", "Fitness", "Sportstudio"],
    "Electricians": ["Electrician", "Elektriker", "Elektro", "Elektroinstallation"],
    "Roofing": ["Roofing", "Dachdecker", "Dach", "Dacharbeiten"],
    "Locksmiths": ["Locksmith", "Schlüsseldienst", "Schlosser", "Schlüssel"],
    "HVAC": ["HVAC", "Heizung Sanitär", "Klima", "Lüftung"],
    "Car wash": ["Car wash", "Autowaschanlage", "Autowaschen", "Waschstraße"],
    "Florists": ["Florist", "Blumenladen", "Floristik", "Blumen"],
    "Bakeries": ["Bakery", "Bäckerei", "Bäckereien", "Brot"],
    "Pharmacies": ["Pharmacy", "Apotheke", "Apotheken"],
    "Optometrists": ["Optometrist", "Optiker", "Augenoptiker", "Brillen"],
    "Insurance agents": ["Insurance", "Versicherung", "Versicherungsmakler", "Versicherer"],
    "Accountants": ["Accountant", "Buchhalter", "Buchhaltung", "Steuerberatung"],
    "Photographers": ["Photographer", "Fotograf", "Fotografie", "Fotostudio"],
    "Dry cleaning": ["Dry cleaning", "Chemische Reinigung", "Reinigung", "Wäscherei"],
    "IT support": ["IT support", "IT Dienstleistung", "EDV", "Computer Service"],
    "Marketing agencies": ["Marketing", "Marketing Agentur", "Werbung", "Agentur"],
    "Catering": ["Catering", "Catering Service", "Partyservice", "Event Catering"],
    "Web design agencies": ["Web design agency", "Webdesign Agentur", "Webagentur", "Website design"],
    "Software development companies": ["Software development", "Softwarefirma", "Softwareentwicklung", "App development"],
    "Managed IT services": ["Managed IT services", "IT Betreuung", "IT Service", "IT Wartung"],
    "Cybersecurity services": ["Cybersecurity", "IT Sicherheit", "Cyber Security", "Informationssicherheit"],
    "Business consultants": ["Business consultant", "Unternehmensberatung", "Consulting", "Managementberatung"],
    "Coworking spaces": ["Coworking space", "Co-Working", "Shared office", "Buro auf Zeit"],
    "Translation services": ["Translation service", "Übersetzungsbüro", "Dolmetscher", "Übersetzung"],
    "Funeral services": ["Funeral service", "Bestattungsunternehmen", "Bestatter", "Beerdigung"],
    "Tattoo studios": ["Tattoo studio", "Tattoo", "Tatowierstudio", "Tätowierung"],
    "Nail salons": ["Nail salon", "Nagelstudio", "Maniküre", "Pediküre"],
    "Barbershops": ["Barbershop", "Barber", "Herrenfriseur", "Bartpflege"],
    "Massage therapists": ["Massage therapist", "Massagestudio", "Massagepraxis", "Therapeutische Massage"],
    "Podiatrists": ["Podiatrist", "Podologie", "Podologe", "Fußpflege medizinisch"],
    "Veterinary clinics": ["Veterinary clinic", "Tierklinik", "Tierarztpraxis", "Veterinar"],
    "Dental labs": ["Dental lab", "Dentallabor", "Zahnlabor", "Dentaltechnik"],
    "Orthodontists": ["Orthodontist", "Kieferorthopäde", "Kieferorthopadie", "Zahnspange"],
    "Private schools": ["Private school", "Privatschule", "International school", "Ersatzschule"],
    "Language schools": ["Language school", "Sprachschule", "Deutschkurs", "Englischkurs"],
    "Driving schools": ["Driving school", "Fahrschule", "Fahrunterricht", "Fahrerlaubnis"],
    "Childcare centers": ["Childcare center", "Kindertagesstätte", "Kita", "Kinderbetreuung"],
    "Event venues": ["Event venue", "Eventlocation", "Veranstaltungsort", "Event space"],
    "Wedding planners": ["Wedding planner", "Hochzeitsplaner", "Wedding service", "Hochzeitsagentur"],
    "Home cleaning services": ["Home cleaning service", "Haushaltsreinigung", "Putzservice", "Reinigungsfirma"],
    "Solar installers": ["Solar installer", "Photovoltaik", "Solaranlage", "Solartechnik"],
    "Painters": ["Painter", "Malerbetrieb", "Maler", "Lackierer"],
    "Handyman services": ["Handyman", "Hausmeisterservice", "Reparaturservice", "Allrounder Service"],
    "Kitchen studios": ["Kitchen studio", "Küchenstudio", "Einbauküche", "Küchenplanung"],
    "Furniture stores": ["Furniture store", "Möbelhaus", "Möbelgeschäft", "Einrichtungshaus"],
    "Architects": ["Architect", "Architekt", "Architekturbüro", "Bauplanung"],
    "Interior designers": ["Interior designer", "Innenarchitekt", "Raumgestaltung", "Interior design studio"],
    "General contractors": ["General contractor", "Generalunternehmer", "Bauunternehmen", "Bauprojekt"],
    "Security system installers": ["Security system installer", "Sicherheitstechnik", "Alarmanlage", "Videoüberwachung"],
    "Signage companies": ["Signage company", "Werbetechnik", "Schilder", "Beschriftung"],
    "Print shops": ["Print shop", "Druckerei", "Druckservice", "Digitaldruck"],
    "Copy shops": ["Copy shop", "Kopiershop", "Kopierladen", "Kopiendienst"],
    "Recruitment agencies": ["Recruitment agency", "Personalvermittlung", "Headhunter", "Arbeitsvermittlung"],
    "HR consultants": ["HR consultant", "Personalberater", "HR Beratung", "People consulting"],
    "Payroll services": ["Payroll service", "Lohnbuchhaltung", "Gehaltsabrechnung", "Payroll"],
    "E-commerce agencies": ["E-commerce agency", "Ecommerce Agentur", "Online shop agency", "Shopify Agentur"],
    "Video production companies": ["Video production", "Videoproduktion", "Filmproduktion", "Imagefilm"],
    "PR agencies": ["PR agency", "Public relations", "Pressearbeit", "Kommunikationsagentur"],
    "B2B sales consultants": ["B2B sales consultant", "Vertriebsberatung", "Sales consulting", "Business development consultant"],
    "Logistics companies": ["Logistics company", "Logistikunternehmen", "Transportlogistik", "Spedition"],
    "Courier services": ["Courier service", "Kurierdienst", "Expressversand", "Bote"],
    "Storage facilities": ["Storage facility", "Lagerraum", "Self storage", "Lagerhaus"],
    "Medical clinics": ["Medical clinic", "Arztpraxis", "Gesundheitszentrum", "Klinik"],
    "Dermatologists": ["Dermatologist", "Dermatologie", "Hautarzt", "Hautklinik"],
    "Chiropractors": ["Chiropractor", "Chiropraktiker", "Chiropraktik", "Wirbelsäulentherapie"],
    "Speech therapists": ["Speech therapist", "Logopädie", "Logopäde", "Sprachtherapie"],
    "Occupational therapists": ["Occupational therapist", "Ergotherapie", "Ergotherapeut", "Handtherapie"],
    "Orthopedic clinics": ["Orthopedic clinic", "Orthopädie", "Orthopäde", "Gelenkzentrum"],
    "Psychologists": ["Psychologist", "Psychologe", "Psychotherapie", "Therapiepraxis"],
    "Denturists": ["Denturist", "Prothetik", "Zahnersatz", "Dentalprothetik"],
    "Car dealerships": ["Car dealership", "Autohaus", "Neuwagen", "Gebrauchtwagen"],
    "Tire shops": ["Tire shop", "Reifenservice", "Reifenhandel", "Reifenwechsel"],
    "Window cleaning services": ["Window cleaning", "Fensterreinigung", "Glasreinigung", "Fensterputzer"],
    "Pest control services": ["Pest control", "Schädlingsbekämpfung", "Kammerjäger", "Pest service"],
    "Security guard services": ["Security guard service", "Sicherheitsdienst", "Objektschutz", "Wachdienst"],
    "Yoga studios": ["Yoga studio", "Yoga", "Yogastudio", "Hatha Yoga"],
    "Pilates studios": ["Pilates studio", "Pilates", "Pilatesstudio", "Reformer Pilates"],
    "CrossFit gyms": ["CrossFit", "CrossFit gym", "Crossfit Box", "Functional fitness"],
    "Martial arts schools": ["Martial arts school", "Kampfsportschule", "Karateschule", "Kampfsport"],
    "Dance studios": ["Dance studio", "Tanzschule", "Tanzstudio", "Ballettschule"],
    "Music schools": ["Music school", "Musikschule", "Musikunterricht", "Musikakademie"],
    "Art schools": ["Art school", "Kunstschule", "Malschule", "Zeichenschule"],
    "Bookstores": ["Bookstore", "Buchhandlung", "Bücher", "Buchladen"],
    "Electronics stores": ["Electronics store", "Elektronikgeschäft", "Elektrofachhandel", "Media Markt"],
    "Hardware stores": ["Hardware store", "Baumarkt", "Heimwerker", "DIY store"],
    "Garden centers": ["Garden center", "Gartencenter", "Gärtnerei", "Pflanzenmarkt"],
    "Pet stores": ["Pet store", "Tierbedarf", "Zoohandlung", "Tierhandlung"],
    "Toy stores": ["Toy store", "Spielwaren", "Spielzeugladen", "Kinderspielzeug"],
    "Shoe stores": ["Shoe store", "Schuhgeschäft", "Schuhe", "Schuhladen"],
    "Jewelry stores": ["Jewelry store", "Juwelier", "Schmuck", "Goldschmied"],
    "Phone repair shops": ["Phone repair", "Handy Reparatur", "Smartphone Reparatur", "Display Reparatur"],
    "Computer repair shops": ["Computer repair", "PC Reparatur", "Laptop Reparatur", "IT Reparatur"],
    "Appliance repair services": ["Appliance repair", "Haushaltsgeräte Reparatur", "Waschmaschinen Reparatur", "Gerätereparatur"],
    "Bicycle shops": ["Bicycle shop", "Fahrradladen", "Fahrradgeschäft", "Radladen"],
    "Motorcycle dealerships": ["Motorcycle dealership", "Motorradhändler", "Motorradhandel", "Zweirad"],
    "Gas stations": ["Gas station", "Tankstelle", "Benzinstelle", "Autotankstelle"],
    "Laundromats": ["Laundromat", "Waschsalon", "Münzwäscherei", "Wäscherei"],
    "Tailors": ["Tailor", "Schneider", "Schneiderei", "Änderungsschneiderei"],
    "Party supply stores": ["Party supply", "Partyzubehör", "Partygeschäft", "Festbedarf"],
    "Office supply stores": ["Office supply", "Bürobedarf", "Schreibwaren", "Office Depot"],
    "Home health care services": ["Home health care", "Pflegedienst", "Ambulante Pflege", "Krankenpflege zu Hause"],
    "Urgent care centers": ["Urgent care", "Notfallpraxis", "Walk-in Klinik", "Medizinisches Versorgungszentrum"],
    "Walk-in clinics": ["Walk-in clinic", "Walk-in Praxis", "Ohne Termin", "Sprechstunde"],
    "Medical laboratories": ["Medical laboratory", "Medizinisches Labor", "Labor", "Blutlabor"],
    "Audiologists": ["Audiologist", "Hörgeräteakustiker", "Audiologie", "Hörzentrum"],
    "Hearing aid centers": ["Hearing aid center", "Hörgeräte", "Hörgeräteakustik", "Hörstudio"],
    "Ophthalmologists": ["Ophthalmologist", "Augenarzt", "Augenheilkunde", "Augenklinik"],
    "Cardiologists": ["Cardiologist", "Kardiologe", "Herzspezialist", "Herzpraxis"],
    "Family doctors / GPs": ["Family doctor", "Hausarzt", "Allgemeinarzt", "GP"],
    "Pediatricians": ["Pediatrician", "Kinderarzt", "Pädiatrie", "Kinderärztin"],
    "Psychiatrists": ["Psychiatrist", "Psychiater", "Psychiatrie", "Nervenarzt"],
    "Nutritionists": ["Nutritionist", "Ernährungsberater", "Diätassistent", "Ernährungsberatung"],
    "Acupuncture clinics": ["Acupuncture", "Akupunktur", "TCM", "Traditionelle Chinesische Medizin"],
    "Medical spas": ["Medical spa", "Medizinische Wellness", "Ästhetische Medizin", "Med Spa"],
    "Day spas": ["Day spa", "Spa", "Wellness Spa", "Massage Spa"],
    "Breweries": ["Brewery", "Brauerei", "Craft beer", "Bierbrauerei"],
    "Bars & pubs": ["Bar", "Pub", "Kneipe", "Cocktailbar"],
    "Nightclubs": ["Nightclub", "Club", "Diskothek", "Nachtclub"],
    "Food trucks": ["Food truck", "Imbisswagen", "Street Food", "Foodtruck"],
    "Bowling alleys": ["Bowling alley", "Bowlingbahn", "Bowling Center", "Kegelbahn"],
    "Golf courses": ["Golf course", "Golfclub", "Golfplatz", "Golfanlage"],
    "Escape rooms": ["Escape room", "Exit Game", "Live Escape", "Rätselraum"],
    "Movie theaters": ["Movie theater", "Kino", "Filmtheater", "Multiplex"],
    "Swimming schools": ["Swimming school", "Schwimmschule", "Schwimmkurs", "Schwimmunterricht"],
    "Tennis clubs": ["Tennis club", "Tennisclub", "Tennisanlage", "Tennisplatz"],
    "Metal fabrication shops": ["Metal fabrication", "Metallbau", "Schlosserei", "Blechbearbeitung"],
    "Welding services": ["Welding service", "Schweißservice", "Schweißen", "Schweißbetrieb"],
    "CNC machining shops": ["CNC machining", "CNC Fräsen", "Zerspanung", "Präzisionsmechanik"],
    "3D printing services": ["3D printing", "3D Druck", "Additive Fertigung", "Rapid Prototyping"],
    "Packaging companies": ["Packaging company", "Verpackung", "Verpackungsfirma", "Emballage"],
    "Shipping centers": ["Shipping center", "Paketshop", "DHL Paket", "Versandzentrum"],
    "Freight forwarders": ["Freight forwarder", "Spedition", "Frachtführer", "Logistikdienst"],
    "Waste management companies": ["Waste management", "Entsorgung", "Abfallwirtschaft", "Müllentsorgung"],
    "Recycling centers": ["Recycling center", "Recyclinghof", "Wertstoffhof", "Schrottplatz"],
    "Auto body shops": ["Auto body shop", "Karosseriebau", "Unfallinstandsetzung", "Lackiererei"],
    "Auto glass repair": ["Auto glass repair", "Autoglas", "Windschutzscheibe", "Steinschlag Reparatur"],
    "RV parks": ["RV park", "Wohnmobilstellplatz", "Campingplatz", "Stellplatz"],
    "Campgrounds": ["Campground", "Camping", "Zeltplatz", "Campingplatz"],
    "Bed and breakfasts": ["Bed and breakfast", "Pension", "Gästehaus", "BnB"],
    "Hostels": ["Hostel", "Jugendherberge", "Backpacker", "Hostel"],
    "Vacation rental managers": ["Vacation rental", "Ferienwohnung Verwaltung", "Holiday rental", "Airbnb management"],
    "Property management companies": ["Property management", "Hausverwaltung", "Immobilienverwaltung", "Gebäudemanagement"],
    "Land surveyors": ["Land surveyor", "Vermessungsingenieur", "Vermessungsbüro", "Geometer"],
    "Environmental consultants": ["Environmental consultant", "Umweltberatung", "Umwelttechnik", "Ökologie Beratung"],
    "Patent attorneys": ["Patent attorney", "Patentanwalt", "Patentrecht", "IP Anwalt"],
    "Immigration lawyers": ["Immigration lawyer", "Einwanderungsrecht", "Aufenthaltsrecht", "Migrationsrecht"],
    "Notaries": ["Notary", "Notar", "Notariat", "Beurkundung"],
    "Mediation services": ["Mediation", "Mediation service", "Schlichtung", "Konfliktberatung"],
    "Smoke shops": ["Smoke shop", "Tabakladen", "Tabakwaren", "Zigarren"],
    "Vape shops": ["Vape shop", "E-Zigaretten", "Dampfer Shop", "Vaping"],
    "Farm equipment dealers": ["Farm equipment", "Landmaschinen", "Agrartechnik", "Traktoren"],
    "Aquariums & pet fish stores": ["Aquarium store", "Aquaristik", "Zierfische", "Aquarium shop"],
    "Sports equipment stores": ["Sports equipment", "Sportgeschäft", "Sportartikel", "Sportladen"],
    "Musical instrument stores": ["Musical instrument store", "Musikinstrumente", "Musikgeschäft", "Gitarrenladen"],
    "Antique stores": ["Antique store", "Antiquitäten", "Antikladen", "Vintage shop"],
    "Thrift stores": ["Thrift store", "Second Hand", "Gebrauchtwaren", "Flohmarkt Laden"],
    "Consignment shops": ["Consignment shop", "Kommissionsladen", "Second Hand Boutique", "An- und Verkauf"],
    "Pawn shops": ["Pawn shop", "Pfandhaus", "Leihhaus", "Pfandleihe"],
    "Gun stores & shooting ranges": ["Gun store", "Waffenhandel", "Schießstand", "Jagdwaffen"],
    "Hunting & outdoor stores": ["Hunting store", "Outdoor Shop", "Jagdbedarf", "Outdoor Ausrüstung"],
    "Camping gear stores": ["Camping store", "Campingausrüstung", "Outdoor Camping", "Zeltgeschäft"],
    "Ski & snowboard shops": ["Ski shop", "Snowboard", "Skiverleih", "Wintersport"],
    "Surf shops": ["Surf shop", "Surfen", "Surfboard", "Wassersport"],
    "Scuba dive shops": ["Scuba diving", "Tauchshop", "Tauchschule", "Tauchcenter"],
    "Boat repair services": ["Boat repair", "Bootswerft", "Bootsreparatur", "Marine Service"],
    "Marinas": ["Marina", "Yachthafen", "Bootssteg", "Hafen"],
    "Flight schools": ["Flight school", "Flugschule", "Pilotenausbildung", "PPL"],
    "Trucking companies": ["Trucking company", "Spedition", "LKW Transport", "Fuhrunternehmen"],
    "Taxi services": ["Taxi service", "Taxiunternehmen", "Taxizentrale", "Taxi"],
    "Limousine services": ["Limousine service", "Limousinenservice", "Stretch Limo", "Chauffeur"],
    "Charter bus services": ["Charter bus", "Reisebus", "Busvermietung", "Linienbus"],
    "Airport shuttle services": ["Airport shuttle", "Flughafentransfer", "Shuttle Service", "Airport Transfer"],
    "Parking lot operators": ["Parking operator", "Parkhaus", "Parkplatz", "Parkservice"],
    "EV charging installers": ["EV charging", "Ladestation", "E-Mobilität", "Wallbox Installation"],
    "Battery stores": ["Battery store", "Batterien", "Akkus", "Autobatterie"],
    "Lighting stores": ["Lighting store", "Lampen", "Leuchten", "Beleuchtung"],
    "Flooring stores": ["Flooring store", "Bodenbelag", "Parkett", "Laminat"],
    "Carpet stores": ["Carpet store", "Teppich", "Teppichgeschäft", "Teppichboden"],
    "Tile & stone showrooms": ["Tile showroom", "Fliesen", "Naturstein", "Feinsteinzeug"],
    "Window treatment companies": ["Window treatment", "Sonnenschutz", "Rollos", "Jalousien"],
    "Blinds & shutters installers": ["Blinds installer", "Plissee", "Rollladen", "Fensterläden"],
    "Pool builders": ["Pool builder", "Poolbau", "Schwimmbadbau", "Pools"],
    "Pool cleaning services": ["Pool cleaning", "Poolpflege", "Pool Service", "Wasserpflege"],
    "Hot tub dealers": ["Hot tub", "Whirlpool", "Jacuzzi", "Outdoor Spa"],
    "Sauna installers": ["Sauna installer", "Saunabau", "Sauna", "Infrarotkabine"],
    "Fence contractors": ["Fence contractor", "Zaunbau", "Gartenzaun", "Zaun"],
    "Deck builders": ["Deck builder", "Terrassenbau", "Holzterrasse", "Decking"],
    "Paving contractors": ["Paving contractor", "Pflasterarbeiten", "Asphaltierung", "Straßenbau"],
    "Asphalt contractors": ["Asphalt contractor", "Asphalt", "Teerarbeiten", "Belag"],
    "Concrete contractors": ["Concrete contractor", "Betonbau", "Betonarbeiten", "Stahlbeton"],
    "Masonry contractors": ["Masonry contractor", "Maurer", "Mauerwerk", "Steinmetz"],
    "Demolition contractors": ["Demolition contractor", "Abbruch", "Rückbau", "Abriss"],
    "Excavation contractors": ["Excavation contractor", "Erdarbeiten", "Baggerarbeiten", "Aushub"],
    "Crane rental services": ["Crane rental", "Kranvermietung", "Autokran", "Kran"],
    "Scaffolding companies": ["Scaffolding", "Gerüstbau", "Gerüst", "Fassadengerüst"],
    "Construction equipment rental": ["Construction equipment rental", "Baumaschinenverleih", "Bagger mieten", "Baumaschinen"],
    "Tool rental shops": ["Tool rental", "Werkzeugverleih", "Baustellenbedarf", "Mietwerkzeug"],
    "Party equipment rental": ["Party equipment rental", "Partymieten", "Festmieten", "Event Equipment"],
    "Tent rental services": ["Tent rental", "Zeltverleih", "Festzelt", "Pagodenzelt"],
    "AV equipment rental": ["AV rental", "Tonanlage mieten", "Beamer", "Eventtechnik"],
    "Stage & event rental": ["Stage rental", "Bühnenbau", "Eventbühne", "Traverse"],
    "Uniform suppliers": ["Uniform supplier", "Berufsbekleidung", "Arbeitskleidung", "Uniformen"],
    "Workwear stores": ["Workwear store", "Arbeitskleidung", "Sicherheitsschuhe", "Schutzkleidung"],
    "Safety equipment suppliers": ["Safety equipment", "Sicherheitsausrüstung", "PSA", "Arbeitsschutz"],
    "Industrial supply stores": ["Industrial supply", "Industriebedarf", "Gewerbebedarf", "Werkstattbedarf"],
    "Laboratory supply companies": ["Laboratory supply", "Laborbedarf", "Labortechnik", "Reagenzien"],
    "Dental supply companies": ["Dental supply", "Dentalbedarf", "Zahnarztbedarf", "Labor Dental"],
    "Veterinary supply companies": ["Veterinary supply", "Tierarztbedarf", "Vet Supply", "Tierbedarf"],
    "Restaurant supply stores": ["Restaurant supply", "Gastronomiebedarf", "Küchenbedarf", "Gastro"],
    "Commercial kitchen equipment": ["Commercial kitchen equipment", "Großküchentechnik", "Gastroküche", "Küchengeräte"],
    "Coffee roasteries": ["Coffee roastery", "Kaffeerösterei", "Röstkaffee", "Coffee roasting"],
    "Tea shops": ["Tea shop", "Teehaus", "Teeladen", "Tee"],
    "Juice bars": ["Juice bar", "Saftbar", "Smoothie Bar", "Frischsaft"],
    "Smoothie shops": ["Smoothie shop", "Smoothie", "Smoothie Bar", "Fruchtbar"],
    "Ice cream shops": ["Ice cream shop", "Eisdiele", "Eis", "Speiseeis"],
    "Gelato shops": ["Gelato", "Gelateria", "Italienisches Eis", "Eis"],
    "Chocolate shops": ["Chocolate shop", "Schokolade", "Schokoladenladen", "Pralinen"],
    "Candy stores": ["Candy store", "Süßwaren", "Bonbonladen", "Laden"],
    "Butcher shops": ["Butcher shop", "Metzgerei", "Fleischer", "Fleischerei"],
    "Fish markets": ["Fish market", "Fischhandel", "Fischladen", "Fisch"],
    "Organic grocery stores": ["Organic grocery", "Bio Supermarkt", "Bio Laden", "Naturkost"],
    "Health food stores": ["Health food store", "Reformhaus", "Naturkost", "Vitamine"],
    "Vitamin & supplement stores": ["Vitamin store", "Nahrungsergänzung", "Supplements", "Supplementshop"],
    "CBD shops": ["CBD shop", "CBD", "Cannabidiol", "Hanf"],
    "Costume stores": ["Costume store", "Kostümverleih", "Kostüme", "Fasching"],
    "Bridal shops": ["Bridal shop", "Brautmode", "Brautladen", "Hochzeitskleid"],
    "Formal wear rental": ["Formal wear rental", "Anzugverleih", "Smoking Verleih", "Gala"],
    "Luggage stores": ["Luggage store", "Koffer", "Reisegepäck", "Taschen"],
    "Travel agencies": ["Travel agency", "Reisebüro", "Reiseveranstalter", "Urlaub"],
    "Tour operators": ["Tour operator", "Reiseveranstalter", "Touren", "Tourismus"],
    "Cruise travel agencies": ["Cruise agency", "Kreuzfahrt", "Reisebüro Kreuzfahrt", "Schiffsreise"],
    "Visa & passport services": ["Visa service", "Passfoto", "Visa Agentur", "Reisevisum"],
    "Pack & ship centers": ["Pack and ship", "Paketshop", "Versand", "DHL Shop"],
    "Mailbox & shipping stores": ["Mailbox", "Postfach", "Postfachservice", "Paketshop"],
    "Check cashing services": ["Check cashing", "Schecks", "Geldtransfer", "Wechsel"],
    "Currency exchange services": ["Currency exchange", "Wechselstube", "Geldwechsel", "Forex"],
    "Bail bond agencies": ["Bail bonds", "Kaution", "Bürgschaft", "Bail"],
    "Private investigators": ["Private investigator", "Privatdetektiv", "Detektei", "Ermittler"],
    "Security consulting firms": ["Security consulting", "Sicherheitsberatung", "Risikoanalyse", "Security Consultant"],
    "Fire protection services": ["Fire protection", "Brandschutz", "Feuerlöscher", "Brandmelde"],
    "Fire sprinkler contractors": ["Fire sprinkler", "Sprinkleranlage", "Brandschutztechnik", "Wasserlöschanlage"],
    "Elevator service companies": ["Elevator service", "Aufzug", "Fahrstuhl Wartung", "Lift Service"],
    "Building inspection services": ["Building inspection", "Bauabnahme", "Gutachter", "Bau"],
    "Home inspectors": ["Home inspector", "Immobiliengutachter", "Hauskauf", "Hausprüfung"],
    "Energy auditors": ["Energy auditor", "Energieberater", "Energieausweis", "Sanierung"],
    "Insulation contractors": ["Insulation contractor", "Dämmung", "Wärmedämmung", "Dämmstoffe"],
    "Waterproofing companies": ["Waterproofing", "Abdichtung", "Kellerabdichtung", "Feuchteschutz"],
    "Foundation repair companies": ["Foundation repair", "Kellersanierung", "Fundament", "Setzung"],
    "Septic tank services": ["Septic tank", "Abwasser", "Kläranlage", "Fäkal"],
    "Chimney sweeps": ["Chimney sweep", "Schornsteinfeger", "Kaminkehrer", "Kamin"],
    "Gutter cleaning services": ["Gutter cleaning", "Dachrinne", "Rinnenreinigung", "Entwässerung"],
    "Pressure washing companies": ["Pressure washing", "Hochdruckreiniger", "Fassadenreinigung", "Reinigung"],
    "Snow removal services": ["Snow removal", "Schneeräumung", "Winterdienst", "Schnee"],
    "Landscaping supply yards": ["Landscaping supply", "Gartenbedarf", "Erde", "Splitt"],
    "Tree service companies": ["Tree service", "Baumpflege", "Baumfällung", "Forstarbeiten"],
    "Arborists": ["Arborist", "Baumkontrolleur", "Baumgutachter", "Baum"],
    "Lawn care companies": ["Lawn care", "Rasenpflege", "Gartenpflege", "Grünanlagen"],
    "Irrigation contractors": ["Irrigation contractor", "Bewässerung", "Sprinkler", "Rasensprenger"],
    "Sod & turf suppliers": ["Sod supplier", "Rollrasen", "Rasen", "Turf"],
    "Equestrian centers": ["Equestrian center", "Reiterhof", "Reitschule", "Pferdestall"],
    "Dog trainers": ["Dog trainer", "Hundeschule", "Hundetraining", "Welpenkurs"],
    "Dog daycare centers": ["Dog daycare", "Hundetagesstätte", "Doggy Daycare", "Hundebetreuung"],
    "Pet grooming salons": ["Pet grooming", "Hundesalon", "Tiersalon", "Fellpflege"],
    "Pet boarding kennels": ["Pet boarding", "Tierpension", "Hundepension", "Tierhotel"],
    "Aquarium maintenance services": ["Aquarium maintenance", "Aquarium Service", "Aquarienpflege", "Wasserwechsel"],
    "Taxidermy studios": ["Taxidermy", "Präparation", "Tierpräparator", "Trophäen"],
    "Mini golf courses": ["Mini golf", "Minigolf", "Minigolfanlage", "Adventure Golf"],
    "Trampoline parks": ["Trampoline park", "Trampolinhalle", "Jump Park", "Indoor Trampolin"],
    "Indoor playground centers": ["Indoor playground", "Indoorspielplatz", "Kinderland", "Spielhalle"],
    "Laser tag arenas": ["Laser tag", "Lasertag", "Laser Game", "Indoor Lasertag"],
    "Go-kart tracks": ["Go kart", "Gokart", "Kartbahn", "Indoor Kart"],
    "Racquet sports clubs": ["Racquet club", "Tennisclub", "Squash", "Badminton Verein"],
    "Rowing clubs": ["Rowing club", "Ruderverein", "Ruderclub", "Rudern"],
    "Sailing schools": ["Sailing school", "Segelschule", "Segelkurs", "Yachting"],
    "Fishing charter services": ["Fishing charter", "Angeln Charter", "Boot angeln", "Guided Fishing"],
    "Embroidery shops": ["Embroidery", "Stickerei", "Sticken", "Textilveredelung"],
    "Screen printing shops": ["Screen printing", "Siebdruck", "Textildruck", "Druckwerkstatt"],
    "Trophy & awards shops": ["Trophy shop", "Pokale", "Medaillen", "Auszeichnung"],
    "Engraving services": ["Engraving", "Gravur", "Lasergravur", "Schildergravur"],
    "Document shredding services": ["Document shredding", "Aktenvernichtung", "Datenschutz Vernichtung", "Schredder"],
    "Records storage services": ["Records storage", "Aktenlager", "Archivierung", "Dokumentenlager"],
    "Medical courier services": ["Medical courier", "Medizinischer Kurier", "Labor Transport", "Bluttransport"],
    "Medical waste disposal": ["Medical waste", "Klinikabfall", "Medizinische Entsorgung", "Bioabfall"],
    "Mold remediation services": ["Mold remediation", "Schimmelbeseitigung", "Schimmel Sanierung", "Feuchteschaden"],
    "Asbestos abatement contractors": ["Asbestos abatement", "Asbestentsorgung", "Asbestsanierung", "Asbest"],
    "Radon mitigation services": ["Radon mitigation", "Radon", "Radonsanierung", "Messung Radon"],
    "Air duct cleaning services": ["Air duct cleaning", "Lüftungsreinigung", "Kanalreinigung", "Klima Kanal"],
    "Carpet cleaning companies": ["Carpet cleaning", "Teppichreinigung", "Polsterreinigung", "Teppich"],
    "Upholstery cleaning services": ["Upholstery cleaning", "Polsterreinigung", "Möbelreinigung", "Stoff"],
    "Furniture restoration shops": ["Furniture restoration", "Möbelrestaurierung", "Antik Restaurierung", "Restaurator"],
    "Piano tuners": ["Piano tuner", "Klavierstimmer", "Stimmung Klavier", "Klavier"],
    "Musical instrument repair shops": ["Instrument repair", "Instrumentenreparatur", "Geigenbau", "Blasinstrument"],
    "Watch repair shops": ["Watch repair", "Uhrenreparatur", "Uhrmacher", "Armbanduhr"],
    "Jewelry repair services": ["Jewelry repair", "Schmuckreparatur", "Goldschmied Reparatur", "Ring"],
    "Mattress stores": ["Mattress store", "Matratzen", "Bettenhaus", "Schlaf"],
    "Bedding stores": ["Bedding store", "Bettwäsche", "Heimtextilien", "Linen"],
    "Sewing & craft stores": ["Sewing store", "Nähbedarf", "Stoffladen", "Handarbeit"],
    "Hobby shops": ["Hobby shop", "Hobbyladen", "Modellbau", "Basteln"],
    "Comic book stores": ["Comic book store", "Comicladen", "Manga Shop", "Comics"],
    "Collectibles shops": ["Collectibles", "Sammlerladen", "Sammelkarten", "Memorabilia"],
    "Vinyl record stores": ["Record store", "Plattenladen", "Schallplatten", "Vinyl"],
    "Video game stores": ["Video game store", "Gameshop", "Konsolen", "Spiele"],
    "Electronics recycling centers": ["Electronics recycling", "Elektroschrott", "WEEE", "Recycling Elektronik"],
    "Mobile phone stores": ["Mobile phone store", "Handyshop", "Smartphone Shop", "Telekom Shop"],
    "Home theater installers": ["Home theater", "Heimkino", "Surround Installation", "Beamer"],
    "Smart home installers": ["Smart home", "Hausautomation", "KNX", "Smarthome"],
    "Satellite TV installers": ["Satellite installer", "Sat Anlage", "Satellitenschüssel", "TV Montage"],
    "Internet service providers": ["Internet provider", "ISP", "DSL Anbieter", "Glasfaser"],
    "Shoe repair shops": ["Shoe repair", "Schuhreparatur", "Schuster", "Schuhe"],
    "Leather repair shops": ["Leather repair", "Lederreparatur", "Lederverarbeitung", "Taschen Reparatur"],
    "Cosmetic dentists": ["Cosmetic dentist", "Ästhetische Zahnheilkunde", "Bleaching", "Veneers"],
    "Endodontists": ["Endodontist", "Wurzelbehandlung", "Endodontie", "Zahnarzt Wurzel"],
    "Periodontists": ["Periodontist", "Parodontologie", "Zahnfleisch", "Implantologie"],
    "Prosthodontists": ["Prosthodontist", "Zahnprothetik", "Prothetik", "Zahnersatz"],
    "Oral surgeons": ["Oral surgeon", "Mund Kiefer Gesicht", "MKG", "Zahnchirurg"],
    "Family practice doctors": ["Family practice", "Hausarztpraxis", "Allgemeinmedizin", "Internist"],
    "Urologists": ["Urologist", "Urologie", "Urologe", "Blase"],
    "Gastroenterologists": ["Gastroenterologist", "Gastroenterologie", "Magen Darm", "Verdauung"],
    "Pulmonologists": ["Pulmonologist", "Pneumologie", "Lungenarzt", "Atemwege"],
    "Endocrinologists": ["Endocrinologist", "Endokrinologie", "Hormone", "Diabetes"],
    "Rheumatologists": ["Rheumatologist", "Rheumatologie", "Gelenke", "Autoimmun"],
    "Nephrologists": ["Nephrologist", "Nephrologie", "Niere", "Dialyse Arzt"],
    "Allergists & immunologists": ["Allergist", "Allergologie", "Allergie", "Immunologie"],
    "Infectious disease specialists": ["Infectious disease", "Infektiologie", "Tropenmedizin", "Infekt"],
    "Pain management clinics": ["Pain management", "Schmerztherapie", "Schmerzambulanz", "Chronische Schmerzen"],
    "Physical rehabilitation centers": ["Physical rehabilitation", "Rehabilitation", "Reha Klinik", "Physikalische Therapie"],
    "Sports medicine clinics": ["Sports medicine", "Sportmedizin", "Sportarzt", "Leistungsdiagnostik"],
    "Fertility clinics": ["Fertility clinic", "Kinderwunsch", "Reproduktionsmedizin", "IVF"],
    "Sleep medicine clinics": ["Sleep medicine", "Schlafmedizin", "Schlaflabor", "Schlafapnoe"],
    "Medical imaging centers": ["Medical imaging", "Bildgebung", "Radiologie Praxis", "MRT"],
    "Dialysis centers": ["Dialysis center", "Dialyse", "Dialysezentrum", "Nierenersatz"],
    "Hospice care services": ["Hospice", "Hospiz", "Palliativ", "Sterbebegleitung"],
    "Home care nursing agencies": ["Home care nursing", "Ambulante Pflege", "Pflegedienst", "24h Pflege"],
    "Medical equipment rental": ["Medical equipment rental", "Medizinprodukte Vermietung", "Krankenbett mieten", "Sanitätshaus"],
}

COLUMNS = [
    "niche",
    "city",
    "business_name",
    "First_Name",
    "phone",
    "google_maps_url",
    "website_url",
    "emails_found",
    "ReviewsCount",
]

if os.environ.get("VERCEL"):
    OUTPUT_DIR = Path("/tmp")
else:
    OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "leads_de_bw.csv"
CHECKPOINT_INTERVAL = 100
MAX_RESULTS_PER_SEARCH = 20
REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
PLACE_DETAIL_WORKERS = 12
EMAIL_EXTRACT_WORKERS = 20
TEXT_SEARCH_MAX_PAGES = 5
PAGE_TOKEN_DELAY = 2.0

DEFAULT_PHONE_COUNTRY_CODE = "+49"


def format_phone_with_country_code(phone: str | None) -> str:
    if not phone or not str(phone).strip():
        return "Nill"
    s = re.sub(r"\s+", " ", str(phone).strip())
    if not s:
        return "Nill"
    if s.startswith("+"):
        return s
    if s.startswith("0"):
        s = s[1:]
    return f"{DEFAULT_PHONE_COUNTRY_CODE} {s}" if s else "Nill"


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(config_path: Path) -> dict:
    config = {}
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
    api_key = (
        config.get("google_api_key")
        or config.get("api_key")
        or os.environ.get("GOOGLE_PLACES_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
    )
    if not api_key or api_key == "YOUR_GOOGLE_API_KEY_HERE":
        logging.error(
            "Please set your Google API key in %s or set GOOGLE_PLACES_API_KEY / GOOGLE_API_KEY env var. "
            "Copy config.example.json to config.json and add your key.",
            config_path,
        )
        sys.exit(1)
    config["google_api_key"] = api_key
    return config


def load_checkpoint(checkpoint_file: Path | None = None) -> tuple[list[dict], set[str], set[str]]:
    cf = checkpoint_file or CHECKPOINT_FILE or CHECKPOINT_FILE
    if not cf.exists():
        return [], set(), set()

    try:
        with open(cf, encoding="utf-8") as f:
            data = json.load(f)
        leads = data.get("leads", [])
        seen_place_ids = set(data.get("seen_place_ids", []))
        seen_domains = set(data.get("seen_domains", []))
        logging.info(
            "Loaded checkpoint: %d leads, %d place_ids, %d domains",
            len(leads), len(seen_place_ids), len(seen_domains),
        )
        return leads, seen_place_ids, seen_domains
    except Exception as e:
        logging.warning("Failed to load checkpoint: %s", e)
        return [], set(), set()


def save_checkpoint(
    leads: list[dict],
    seen_place_ids: set[str],
    seen_domains: set[str],
    checkpoint_file: Path | None = None,
) -> None:
    cf = checkpoint_file or CHECKPOINT_FILE or CHECKPOINT_FILE
    cf.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "leads": leads,
        "seen_place_ids": list(seen_place_ids),
        "seen_domains": list(seen_domains),
    }
    with open(cf, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info("Checkpoint saved: %d leads", len(leads))


def _first_name_only(full: str) -> str:
    if not full or not full.strip():
        return ""
    s = full.strip()
    for sep in (",", " - ", " – ", " | "):
        if sep in s:
            s = s.split(sep)[0].strip()
            if not s:
                s = full.strip()
            break
    words = s.split()
    if not words:
        return ""
    max_words = 2 if any(w.rstrip(".").lower() in ("dr", "prof", "mr", "mrs", "fr") for w in words[:2]) else 1
    short = " ".join(words[:max_words])
    return short[:50].strip()


def get_domain_for_dedup(website: str | None) -> str:
    if not website or not website.strip().startswith("http"):
        return ""
    try:
        netloc = urlparse(website.strip()).netloc
        return netloc.lower() if netloc else ""
    except Exception:
        return ""


def text_search(
    query: str, api_key: str, page_token: str | None = None
) -> tuple[list[dict], str | None]:
    if page_token:
        url = (
            "https://maps.googleapis.com/maps/api/place/textsearch/json"
            f"?pagetoken={quote(page_token)}"
            f"&key={quote(api_key)}"
        )
    else:
        url = (
            "https://maps.googleapis.com/maps/api/place/textsearch/json"
            f"?query={quote(query)}"
            f"&key={quote(api_key)}"
        )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = resp.read().decode()
    except Exception as e:
        logging.debug("Text search error: %s", e)
        return [], None
    try:
        out = json.loads(data)
    except json.JSONDecodeError:
        return [], None
    if out.get("status") not in ("OK", "ZERO_RESULTS"):
        status = out.get("status", "UNKNOWN")
        err_msg = out.get("error_message", "")
        logging.warning("API %s: %s", status, err_msg)
        # Google can return INVALID_REQUEST for a fresh next_page_token
        # until it becomes valid; treat that as non-fatal and stop paging.
        if status == "INVALID_REQUEST" and page_token:
            return [], None
        if status in ("REQUEST_DENIED", "OVER_QUERY_LIMIT", "INVALID_REQUEST"):
            raise GooglePlacesAPIError(
                f"Google Places API {status}: {err_msg or 'Check your API key and that Places API is enabled.'}"
            )
        return [], None
    results = out.get("results", [])
    next_token = out.get("next_page_token") if out.get("status") == "OK" else None
    return results, next_token


def text_search_all_pages(
    query: str, api_key: str, max_pages: int = TEXT_SEARCH_MAX_PAGES
) -> list[dict]:
    all_results: list[dict] = []
    page_token: str | None = None
    for _ in range(max_pages):
        if page_token:
            time.sleep(PAGE_TOKEN_DELAY)
        results, page_token = text_search(query, api_key, page_token=page_token)
        all_results.extend(results)
        if not page_token or not results:
            break
    return all_results


def place_details(place_id: str, api_key: str) -> dict:
    fields = "name,formatted_phone_number,website,url,user_ratings_total"
    url = (
        "https://maps.googleapis.com/maps/api/place/details/json"
        f"?place_id={quote(place_id)}"
        f"&fields={quote(fields)}"
        f"&key={quote(api_key)}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = resp.read().decode()
    except Exception as e:
        logging.debug("Place details error: %s", e)
        return {}
    try:
        out = json.loads(data)
    except json.JSONDecodeError:
        return {}
    if out.get("status") != "OK":
        status = out.get("status", "UNKNOWN")
        err_msg = out.get("error_message", "")
        if status in ("REQUEST_DENIED", "OVER_QUERY_LIMIT", "INVALID_REQUEST"):
            raise GooglePlacesAPIError(
                f"Google Places API {status}: {err_msg or 'Check your API key and that Places API is enabled.'}"
            )
        return {}
    return out.get("result", {})


EMAIL_FALSE_POSITIVE_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg", ".ico",
    ".woff", ".woff2", ".ttf", ".eot", ".otf",
    ".css", ".js", ".map",
)
EMAIL_BLOCKED_DOMAINS = (
    "example.com", "example.org", "sentry.io", "wixpress.com",
    "personio.de", "personio.com", "kzv-sh.de", "google.com",
    "facebook.com", "youtube.com", "twitter.com", "linkedin.com",
    "gravatar.com", "w3.org", "schema.org", "cloudflare.com",
)


def _is_valid_business_email(email: str, website_url: str) -> bool:
    if not email or len(email) < 6 or "@" not in email:
        return False
    e = email.strip().lower()
    if "%" in e or " " in e or "/" in e or "\\" in e:
        return False
    for ext in EMAIL_FALSE_POSITIVE_EXTENSIONS:
        if e.endswith(ext) or ext in e.split("@")[-1]:
            return False
    domain = e.split("@")[-1]
    if re.search(r"\d+w\.(jpeg|jpg|png|webp|gif)", domain):
        return False
    if "." not in domain or len(domain) < 4:
        return False
    for blocked in EMAIL_BLOCKED_DOMAINS:
        if domain == blocked or domain.endswith("." + blocked):
            return False
    return True


def _pick_one_email(emails: list[str], website_url: str) -> str:
    website_domain = ""
    try:
        website_domain = urlparse(website_url.strip()).netloc.lower()
        if website_domain.startswith("www."):
            website_domain = website_domain[4:]
    except Exception:
        pass
    valid = [e for e in emails if _is_valid_business_email(e, website_url)]
    if not valid:
        return ""
    same_domain = [e for e in valid if website_domain and e.split("@")[-1].lower() == website_domain]
    if same_domain:
        for prefix in ("info@", "kontakt@", "buero@", "office@", "mail@", "contact@"):
            for e in same_domain:
                if e.lower().startswith(prefix):
                    return e.strip()
        return same_domain[0].strip()
    return valid[0].strip()


def _extract_owner_from_html(html: str) -> str:
    if not html or len(html) > 5_000_000:
        return ""
    owner = ""
    m = re.search(r'<meta\s+name=["\']author["\']\s+content=["\']([^"\']{2,80})["\']', html, re.I)
    if m:
        candidate = m.group(1).strip()
        if re.match(r"^[\w\s.\-ÄäÖöÜüß']+$", candidate) and len(candidate) >= 2:
            owner = candidate
    if not owner:
        m = re.search(
            r"(?:Inhaber|Geschäftsführer|Inhaberin|Owner|Proprietor)\s*:?\s*([A-Za-zÄäÖöÜüß\-'\s.]{2,60})",
            html,
            re.I,
        )
        if m:
            candidate = m.group(1).strip()
            candidate = re.sub(r"\s+", " ", candidate)
            if len(candidate) >= 2 and not re.search(r"^\d+$", candidate):
                owner = candidate
    if not owner and '"@type":"Person"' in html.replace(" ", ""):
        m = re.search(r'"name"\s*:\s*"([^"]{2,60})"', html)
        if m:
            candidate = m.group(1).strip()
            if re.match(r"^[\w\s.\-ÄäÖöÜüß']+$", candidate):
                owner = candidate
    return owner[:80].strip() if owner else ""


def extract_emails_from_website(
    url: str, sleep_seconds: float = 0.05
) -> tuple[list[str], str, str]:
    if not url or not url.strip().startswith("http"):
        return [], "", ""
    raw = set()
    email_re = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    try:
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        req = urllib.request.Request(url.strip(), headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        for m in email_re.finditer(html):
            e = m.group(0).strip()
            raw.add(e)
        one = _pick_one_email(sorted(raw), url)
        owner = _extract_owner_from_html(html)
        return ([one] if one else [], url, owner)
    except Exception as e:
        logging.debug("Email extraction failed for %s: %s", url, e)
        return [], "", ""


def export_csv(
    leads: list[dict],
    output_path: Path,
    require_email_and_website: bool = False,
) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if require_email_and_website:
        rows = [
            lead for lead in leads
            if lead.get("emails_found") and str(lead.get("emails_found", "")).strip() not in ("", "Nill")
            and lead.get("website_url") and str(lead.get("website_url", "")).strip() not in ("", "Nill")
        ]
        if not rows:
            rows = leads
    else:
        rows = leads
    for lead in rows:
        for col in COLUMNS:
            if col not in lead:
                lead[col] = 0 if col == "ReviewsCount" else ""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logging.info("Exported %d leads to %s", len(rows), output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lead Dataset Builder - Google Places API",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.json"),
        help="Path to config JSON (default: config.json)",
    )
    parser.add_argument(
        "--max-leads",
        type=int,
        default=1000,
        help="Maximum number of leads to collect (default: 1000)",
    )
    parser.add_argument(
        "--extract-emails",
        type=str,
        choices=["true", "false"],
        default="true",
        help="Extract emails from websites; only leads with website+email are kept (default: true)",
    )
    parser.add_argument(
        "--sleep-api",
        type=float,
        default=0.2,
        help="Sleep seconds between text search API calls (default: 0.2)",
    )
    parser.add_argument(
        "--sleep-web",
        type=float,
        default=0.1,
        help="Sleep per website fetch when extracting emails in parallel (default: 0.1)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


def _fetch_details_task(
    item: tuple[str, str, str, str],
    api_key: str,
    seen_place_ids: set[str],
    seen_domains: set[str],
    lock: threading.Lock,
) -> tuple[dict, str] | None:
    place_id, niche, city, name_from_search = item
    details = place_details(place_id, api_key)
    website = (details.get("website") or "").strip()
    if not website or not website.startswith("http"):
        with lock:
            seen_place_ids.add(place_id)
        return None
    domain = get_domain_for_dedup(website)
    with lock:
        if domain and domain in seen_domains:
            seen_place_ids.add(place_id)
            return None
        seen_place_ids.add(place_id)
        if domain:
            seen_domains.add(domain)
    name = details.get("name") or name_from_search or ""
    raw_phone = details.get("formatted_phone_number") or ""
    phone = format_phone_with_country_code(raw_phone)
    maps_url = details.get("url") or ""
    reviews_count = details.get("user_ratings_total")
    if reviews_count is None or (isinstance(reviews_count, (int, float)) and reviews_count < 0):
        reviews_count = 0
    try:
        reviews_count = int(reviews_count)
    except (TypeError, ValueError):
        reviews_count = 0
    lead = {
        "niche": niche,
        "city": city,
        "business_name": name,
        "First_Name": _first_name_only(name),
        "phone": phone,
        "google_maps_url": maps_url,
        "website_url": website,
        "emails_found": "Nill",
        "ReviewsCount": reviews_count,
    }
    return (lead, website)


def _extract_email_task(
    lead_website: tuple[dict, str],
    sleep_web: float,
    leads: list[dict],
    max_leads: int,
    lock: threading.Lock,
    checkpoint_file: Path | None,
    seen_place_ids: set[str],
    seen_domains: set[str],
) -> bool:
    lead, website = lead_website
    try:
        emails, _, owner_name = extract_emails_from_website(website, sleep_seconds=sleep_web)
        one_email = emails[0] if emails else ""
        if not one_email:
            return False
        lead["emails_found"] = one_email
        if owner_name and owner_name.strip():
            lead["First_Name"] = _first_name_only(owner_name.strip())
        with lock:
            if len(leads) >= max_leads:
                return False
            leads.append(lead)
            if len(leads) % CHECKPOINT_INTERVAL == 0 and checkpoint_file:
                save_checkpoint(leads, seen_place_ids, seen_domains, checkpoint_file)
        return True
    except Exception as e:
        logging.debug("Email extraction failed for %s: %s", website, e)
        return False


def run_google(
    api_key: str,
    leads: list[dict],
    seen_place_ids: set[str],
    seen_domains: set[str],
    max_leads: int,
    extract_emails: bool,
    sleep_api: float,
    sleep_web: float,
    checkpoint_file: Path | None = None,
    cities: list[str] | None = None,
    niches: list[str] | None = None,
    max_time_seconds: float | None = None,
) -> None:
    cities = cities if cities is not None else CITIES
    niches = niches if niches is not None else NICHES
    lock = threading.Lock()
    sleep_api = min(sleep_api, 0.25)
    sleep_web = min(sleep_web, 0.15)
    deadline = (time.time() + max_time_seconds) if max_time_seconds else None

    def time_left() -> bool:
        return deadline is None or time.time() < deadline

    for niche in niches:
        if not time_left() or len(leads) >= max_leads:
            break
        for city in cities:
            if not time_left() or len(leads) >= max_leads:
                break
            search_terms = NICHE_SEARCH_VARIATIONS.get(niche, [niche])
            all_results: list[dict] = []
            seen_in_batch: set[str] = set()
            for search_term in search_terms:
                if not time_left() or len(leads) >= max_leads:
                    break
                query = f"{search_term} in {city}"
                logging.info("Collecting: %s in %s (query: %s)", niche, city, query)
                time.sleep(sleep_api)
                results = text_search_all_pages(query, api_key)
                for r in results:
                    place_id = r.get("place_id")
                    if place_id and place_id not in seen_in_batch:
                        seen_in_batch.add(place_id)
                        all_results.append(r)
            batch: list[tuple[str, str, str, str]] = []
            for r in all_results:
                place_id = r.get("place_id")
                if not place_id:
                    continue
                with lock:
                    if place_id in seen_place_ids:
                        continue
                batch.append((place_id, niche, city, r.get("name") or ""))

            if not batch:
                continue

            candidates: list[tuple[dict, str]] = []
            with ThreadPoolExecutor(max_workers=PLACE_DETAIL_WORKERS) as executor:
                futures = {
                    executor.submit(
                        _fetch_details_task,
                        item,
                        api_key,
                        seen_place_ids,
                        seen_domains,
                        lock,
                    ): item
                    for item in batch
                }
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            candidates.append(result)
                    except Exception as e:
                        logging.debug("Place details task error: %s", e)

            if not candidates or len(leads) >= max_leads:
                continue

            with ThreadPoolExecutor(max_workers=EMAIL_EXTRACT_WORKERS) as executor:
                futures = {
                    executor.submit(
                        _extract_email_task,
                        lead_website,
                        sleep_web,
                        leads,
                        max_leads,
                        lock,
                        checkpoint_file,
                        seen_place_ids,
                        seen_domains,
                    ): lead_website
                    for lead_website in candidates
                }
                for future in as_completed(futures):
                    if not time_left() or len(leads) >= max_leads:
                        break
                    try:
                        future.result()
                    except Exception as e:
                        logging.debug("Email task error: %s", e)
            if len(leads) % CHECKPOINT_INTERVAL == 0 and checkpoint_file:
                save_checkpoint(leads, seen_place_ids, seen_domains, checkpoint_file)


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    extract_emails = args.extract_emails.lower() == "true"
    max_leads = args.max_leads

    if args.clear_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logging.info("Checkpoint cleared.")

    leads, seen_place_ids, seen_domains = load_checkpoint()

    if len(leads) >= max_leads:
        logging.info("Already have %d leads (>= max %d). Exporting and exiting.", len(leads), max_leads)
        export_csv(leads, args.output, require_email_and_website=extract_emails)
        return

    logging.info(
        "max_leads=%d, extract_emails=%s, sleep_api=%.2f, sleep_web=%.2f",
        max_leads, extract_emails, args.sleep_api, args.sleep_web,
    )

    config = load_config(args.config)
    api_key = config["google_api_key"]

    run_google(
        api_key=api_key,
        leads=leads,
        seen_place_ids=seen_place_ids,
        seen_domains=seen_domains,
        max_leads=max_leads,
        extract_emails=extract_emails,
        sleep_api=args.sleep_api,
        sleep_web=args.sleep_web,
        checkpoint_file=CHECKPOINT_FILE,
    )

    save_checkpoint(leads, seen_place_ids, seen_domains)
    export_csv(leads, args.output, require_email_and_website=extract_emails)
    logging.info("Done. Total leads: %d", len(leads))


def run_collection_for_city_niche(
    api_key: str,
    city: str,
    niche: str,
    max_leads: int,
    extract_emails: bool = True,
    sleep_api: float = 0.2,
    sleep_web: float = 0.1,
) -> list[dict]:
    return run_collection_for_cities_niches(
        api_key=api_key,
        cities=[city.strip()],
        niches=[niche.strip()],
        max_leads=max_leads,
        extract_emails=extract_emails,
        sleep_api=sleep_api,
        sleep_web=sleep_web,
    )


def run_collection_for_cities_niches(
    api_key: str,
    cities: list[str],
    niches: list[str],
    max_leads: int,
    extract_emails: bool = True,
    sleep_api: float = 0.2,
    sleep_web: float = 0.1,
    max_time_seconds: float | None = None,
) -> list[dict]:
    cities = [c.strip() for c in cities if c and str(c).strip()]
    niches = [n.strip() for n in niches if n and str(n).strip()]
    if not cities or not niches:
        return []
    leads: list[dict] = []
    seen_place_ids: set[str] = set()
    seen_domains: set[str] = set()
    run_google(
        api_key=api_key,
        leads=leads,
        seen_place_ids=seen_place_ids,
        seen_domains=seen_domains,
        max_leads=max_leads,
        extract_emails=extract_emails,
        sleep_api=sleep_api,
        sleep_web=sleep_web,
        checkpoint_file=None,
        cities=cities,
        niches=niches,
        max_time_seconds=max_time_seconds,
    )
    return leads


if __name__ == "__main__":
    main()
