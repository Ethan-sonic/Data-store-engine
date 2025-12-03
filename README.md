# Key-Value Storage Engine u Go-u

Ovaj projekat implementira **key-value engine** za skladištenje velike količine podataka, inspirisan principima modernih baza podataka. Projekat je razvijen kao timski zadatak na fakultetu, a moj doprinos obuhvata implementaciju i optimizaciju ključnih struktura podataka i algoritama.

## Funkcionalnosti
- Skladištenje podataka u obliku **key-value parova**
- Višeslojna arhitektura: keš, memorija, summary strukture, disk
- Efikasno pretraživanje podataka kroz različite slojeve
- **Kompakcija** podataka kada se SStable fajlovi previše uvećaju
- Implementacija probabilističkih algoritama za analizu podataka

## Strukture podataka i algoritmi
- **Bloom filter** – za brzu proveru da li ključ postoji
- **Cache** – sloj za najčešće korišćene podatke
- **Memtable (skip-lista)** – memorijsko skladište podataka pre upisa na disk
- **Write Ahead Log (WAL)** – garantuje trajnost podataka pre nego što se prebace u memtable
- **SStable (Sorted String Table)** – trajno skladište podataka na disku
- **Kompakcija** – spajanje i optimizacija SStable fajlova radi smanjenja prostora i ubrzanja pretrage
- **Count-Min Sketch** – probabilistički algoritam za procenu frekvencije pojavljivanja elemenata
- **HyperLogLog** – algoritam za procenu broja različitih elemenata (cardinality estimation)

## Napomena autora
Projekat je nastao kao timski zadatak na fakultetu. Moj doprinos obuhvata implementaciju i optimizaciju ključnih struktura podataka (skip-lista, Bloom filter, kompakcija) i probabilističkih algoritama (Count-Min Sketch, HyperLogLog). Projekat demonstrira principe na kojima se zasnivaju moderne baze podataka i sistemi za skladištenje podataka.