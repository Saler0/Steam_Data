# Proyecto Steam Data
El proyecto tiene las siguiente estructura:

* Data_management: Contiene lo relacionado al pipeline desde la ingesta de datos hasta la explotation zone
* modelos_steam_data: Contiene lo relacion con los modelos y clustering
* Frontend: Aplicacion Web
* Backend: API que conecte el la aplicacion web con la base de datos.


## 👨‍💻 Para Desarrolladores

Como clonar el repositorio:
```
git clone https://github.com/Saler0/Steam_Data.git
```
Como crear una branch en base a la branch development:
```
git checkout -b <nombre_de_la_branch> development
```

Como subir cambios al repositorio:
```
git add -A
git commit -m "Comentario descriptivo sobre el commit"
git push origin <nombre_de_la_branch>
```

Como taer los ultimos cambios sobre una branch:
```
git pull origin <nombre_de_la_branch>
```

Cuando la branch se encuetra lista para la funcionalidad que fue creada se puede hacer un pull request para poder funcionarla con la rama development:
```
gh pr create --base development --head <nombre_de_la_branch> --title "titulo" --body "body"
```

Como aceptar un pull request:
```
gh pr merge <nombre_de_la_branch>
```

Finalmente para actualizar los cambios del merge en dvelopmeent se hace
```
git checkout development
git pull origin development
```