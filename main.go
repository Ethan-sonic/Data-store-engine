
package main

import (
	"NASP_projekat/Engine"
	"fmt"
)

func main(){
	engine := Engine.NewEngine()

	fmt.Println("Dodavanje 301 elementa.")
	for i := 0; i < 301; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("val%d", i)
		engine.Put(key, []byte(value))
	}

	fmt.Println("Izmena 101 elementa.")
	for i := 0; i < 201; i+=2 {
		key := fmt.Sprintf("key%d", i)
		value := "val13"
		engine.Put(key, []byte(value))
	}

	fmt.Println("Brisanje 41 elementa.")
	for i := 0; i < 201; i+=5 {
		key := fmt.Sprintf("key%d", i)
		engine.Delete(key)
	}

	fmt.Println()
	fmt.Printf("key4: %s\n",string(engine.Get("key4")))		// azuriran
	fmt.Printf("%s\n", string(engine.Get("key105")))			// obrisan
	fmt.Printf("key41: %s\n", string(engine.Get("key41")))	// original

	fmt.Printf("Broj operacija PUT nad kljucem key100: %d\n", engine.CMS.Appearance([]byte("key100")))
	fmt.Printf("Broj razlicitih kljuceva u sistemu: %f\n", engine.HLL.Estimate())

}
