package org.rivenstudio.dynamodemo.controller;

import org.rivenstudio.dynamodemo.entity.Hero;
import org.rivenstudio.dynamodemo.repository.HeroRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class HeroController {
    @Autowired
    private HeroRepository heroRepository;

    @PostMapping("/hero")
    public Hero saveHero(@RequestBody Hero hero) {
        return heroRepository.save(hero);
    }

    @GetMapping("/hero/{id}")
    public Hero getHero(@PathVariable int id) {
        return heroRepository.getHeroById(id);
    }

    @GetMapping("/hero")
    public List<Hero> getHeroList() {
        return heroRepository.findAll();
    }
}
