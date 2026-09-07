FIRST_NAMES = (
    'Ana', 'Bruno', 'Camila', 'Daniel', 'Elisa', 'Felipe', 'Gabriela', 'Henrique',
    'Isabela', 'João', 'Larissa', 'Lucas', 'Mariana', 'Pedro', 'Beatriz', 'Rafael',
    'Juliana', 'Gustavo', 'Fernanda', 'Rodrigo', 'Patrícia', 'André', 'Aline', 'Caio',
    'Clara', 'Diego', 'Eduarda', 'Fábio', 'Helena', 'Igor', 'Jéssica', 'Leandro',
    'Luana', 'Marcelo', 'Natália', 'Otávio', 'Paula', 'Renato', 'Sabrina', 'Thiago',
    'Valéria', 'Vinícius', 'Yasmin', 'Vítor', 'Cecília', 'Davi', 'Evelyn', 'Samuel',
    'Lorena', 'Miguel', 'Mônica', 'Nicolas', 'Priscila', 'Ricardo', 'Sílvia', 'Tiago',
    'Bianca', 'Eduardo', 'Carolina', 'Leonardo', 'Maria Clara', 'Ana Paula', 'João Pedro', 'José Luís',
)
LAST_NAMES = (
    'Silva', 'Santos', 'Oliveira', 'Souza', 'Costa', 'Pereira', 'Lima', 'Almeida',
    'Ferreira', 'Rodrigues', 'Gomes', 'Ribeiro', 'Martins', 'Carvalho', 'Araújo',
    'Melo', 'Barbosa', 'Rocha', 'Dias', 'Nascimento', 'Andrade', 'Moreira', 'Nunes',
    'Marques', 'Machado', 'Mendes', 'Freitas', 'Cardoso', 'Ramos', 'Teixeira',
    'Vieira', 'Castro', 'Azevedo', 'Barros', 'Correia', 'Cavalcanti', 'Batista',
    'Monteiro', 'Moura', 'Pinto', 'Reis', 'Neves', 'Campos', 'Tavares', 'Duarte',
    'Farias', 'Sales', 'Borges', 'Siqueira', 'Guimarães', 'Assis', 'Peixoto',
)

LOCATIONS = (
    ('São Paulo', 'SP', '01001000'), ('Campinas', 'SP', '13010000'),
    ('Rio de Janeiro', 'RJ', '20040000'), ('Curitiba', 'PR', '80010000'),
    ('Belo Horizonte', 'MG', '30110000'), ('Recife', 'PE', '50030000'),
    ('Porto Alegre', 'RS', '90010000'), ('Salvador', 'BA', '40020000'),
    ('Fortaleza', 'CE', '60060000'), ('Manaus', 'AM', '69005000'),
    ('Belém', 'PA', '66010000'), ('Goiânia', 'GO', '74003000'),
    ('Brasília', 'DF', '70040010'), ('Florianópolis', 'SC', '88010000'),
    ('Vitória', 'ES', '29010000'), ('Natal', 'RN', '59025000'),
    ('João Pessoa', 'PB', '58010000'), ('Maceió', 'AL', '57020000'),
    ('Aracaju', 'SE', '49010000'), ('São Luís', 'MA', '65010000'),
    ('Teresina', 'PI', '64000000'), ('Cuiabá', 'MT', '78005000'),
    ('Campo Grande', 'MS', '79002000'), ('Palmas', 'TO', '77001000'),
    ('Porto Velho', 'RO', '76801000'), ('Rio Branco', 'AC', '69900000'),
    ('Boa Vista', 'RR', '69301000'), ('Macapá', 'AP', '68900000'),
)


def person(rng):
    surnames = rng.sample(LAST_NAMES, rng.choices([1, 2, 3], [15, 70, 15])[0])
    return ' '.join([rng.choice(FIRST_NAMES), *surnames])


def address(rng):
    city, state, postal = rng.choice(LOCATIONS)
    street = rng.choice(['Ipês', 'Acácias', 'Horizonte', 'Primavera', 'Jardins', 'Araucárias', 'Paineiras', 'Flamboyants'])
    complement = f', apto {rng.randint(1, 15)}{rng.randint(1, 4):02}' if rng.random() < 0.3 else ''
    return dict(address_line=f'{rng.choice(["Rua", "Avenida", "Alameda"])} {street}, {rng.randint(1, 1800)}{complement}',
                postal_code=postal, city=city, state=state, country_code='BR')


def device(rng):
    return rng.choices([
        ('mobile', 'Chrome', 'Android'), ('mobile', 'Safari', 'iOS'),
        ('mobile', 'Firefox', 'Android'), ('desktop', 'Chrome', 'Windows'),
        ('desktop', 'Edge', 'Windows'), ('desktop', 'Firefox', 'Linux'),
        ('desktop', 'Safari', 'macOS'), ('tablet', 'Safari', 'iPadOS'),
    ], [40, 25, 3, 15, 6, 3, 5, 3])[0]
