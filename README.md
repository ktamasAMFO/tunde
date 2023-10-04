## A projekthez tartozó business knowledge összefoglalva
A projekt az EGIS gyógyszergyártási folyamatában keletkezett adatokkal foglalkozik. Szenzorok és gyártást vezérlő gépek szolgáltatnak adatot egy 5.7-es MySQL adatbázisba, mi innen vesszük ki azokat. A gyártási folyamatról röviden:

A gyógyszereket/hatóanyagokat úgynevezett autoklávokban (unit) keverik ki. Ezek olyan eszközök, amikben kontrollálni lehet a hőmérsékletet, nyomást, stb. A mi szempontunkból leginkább a hőmérséklet szabályozás fontos, ezt különböző hőmérsékletű olajokat keringető fűtőkörrel oldják meg, amire rá vannak kapcsolva az egyes autoklávok. Autoklávokból kb 30 van. Szenzorok vannak a fűtőkörben is és az autoklávokban is, ezekből percenként jönnek adatok az adatbázisba.

A gyógyszerek/hatóanyagok gyártásának folyamatát több szintre lehet osztani. Vannak receptek (recipe), amik több unit receptből (unit recipe) állnak, amik pedig műveletekből (operation) állnak, ezek a legkisebb egységei a gyártási folyamatnak. Egy unit recept alá tartozó műveletek mind ugyanazon az autoklávon történnek meg (ezért is hívják őket unit receptnek). 

Egy konkrét termék elkészítése több (különböző) recept egymásutánjából áll össze. Ezeket a halmazokat hívjuk sarzsoknak (charge/batch(?)). Egy sarzs maximum pár hétig tart. Sajnos a sarzsokról nincs külön tábla amiben látnánk, hogy pontosan mettől meddig tartott, hanem az alá tartozó receptekből lehet visszafejteni ezt. A recepteknek, unit recepteknek és műveleteknek van ilyen információt tartalmazó táblájuk/view-juk.

Két különböző "gyártást" különítenek el: API1 és API2. Minden sarzs, recept, unit recept, művelet valamelyik alá tartozik (ez öröklődik, tehát pl egy sarzsban futó receptek mind ugyanahhoz a gyártáshoz tartoznak), sőt, az egyes autoklávok is fixen valamelyik gyártáshoz tartoznak. A két gyártásnak semmi köze nincs egymáshoz, az adatbázisban vagy egy oszlopból derül ki, hogy az adott sor melyik gyártáshoz tartozik, vagy eleve külön táblában vannak tárolva az API1-es és API2-es adatok.

A gyártásokon van még szó különböző "termékekről". A gyártás pár hónapos ütemekben zajlik, egy ütem alatt általában 1 típusú terméket gyártanak. (Egy sarzsban egy terméket gyártanak, de egy terméket több sarzsban is gyárthatnak.) Ezt a termék azonosítót sajnos nem lehet mindig egyértelműen kideríteni. 

## Futtatási környezet beállítása fejlesztéshez
###### Az apphoz szükséges dependenciák
`docker build -t egis:build-wheels deploy`  
`docker run -d --name build-wheels egis:build-wheels`  
Kimásoljuk a containerből a készített tömörített csomagot  
`docker cp build-wheels:/opt/python/wheels.tar deploy\`  
Megszűntetjük a containert  
`docker rm build-wheels`

###### Távoli adatbázis:
A tgyhanaefr02.egis.hu hoston az *enet_dev* sémán érhető el a fejlesztéshez használható adatbázis.

###### Lokális adatbázis (outdated):
Csinálunk egy volume-ot, amit az adatbázis-image-ek tudnak majd használni  
`docker volume create mysql-data`  
Csinálunk egy docker networkot, amin keresztül tudnak csatlakozni név alapján is a containerek  
`docker network create --driver bridge egis-net`  
Majd legyártjuk a dev adatbázist.  
`docker build -t egis:dev-db dev-db`  
`docker run -p 3307:3306 -v mysql-data:/var/lib/mysql --name dev-db --network egis-net -dit egis:dev-db --sql_mode=""`  
Megnyitjuk a logokat, és megvárjuk, amíg befejezi az adatbázis indítását (~20 perc)  
`docker logs -f dev-db`  
Ezután fejlesztéskor csak el kell indítani a dev-db containert és végül leállítani. A docker containerek a container porton érik el egymást (ez a mysql szervernél 3306),
de a host gépről a host porton érhető el (ez esetben 3307)  
`docker start dev-db`  
`docker stop dev-db`  
Ha újra kell építeni az adatbázist, akkor először a volume-ot és containert törölni kell  
`docker rm -f dev-db`  
`docker volume rm mysql-data`  

###### Dashboard applikáció
Fejlesztéskor a kódot volume-ként csatoljuk a containerekhez. Ha ugyanarra a container mappára irányítjuk a volume-ot ahova a Dockerfile-ban másoltunk fájlokat, akkor azok a fájlok amiket oda tettünk a Dockerfile-ban, elvesznek (kvázi felülíródik a containeren belül az a mappa a host gép megadott mappájával). 
Prod környezetben már nem csatolunk volume-ot a containerhez, ezért a Dockerfile-ban bemásolt kód és config fájlok maradnak meg.
Ezért a config.ini fájlt még build előtt kell beállítani a prod környezetnek megfelelően  
`docker build -t egis:app .`  
Dev környezetben csinálunk egy containert a legyártott image-ből és elindítjuk  
`docker run -p 8050:8050 -v ${pwd}:/app/ --name app --network egis-net -dit egis:app`  
Első indulás után fel kell tölteni statisztikákkal egy táblát  
`docker exec -it app python /app/ts_stats.py`  
Ha valamit változtatunk a kódon, akkor csak újra kell indítani az app containert  
`docker restart app`  
