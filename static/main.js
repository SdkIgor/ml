function sortByValue(jsObj){
    var sortedArray = [];
    for(var i in jsObj) {
        sortedArray.push([jsObj[i], i]);
    }
    return sortedArray.sort();
}

function createSelectOpts(data, id2clean) {
  $(id2clean).empty();
  // for (const [key, value] of Object.entries(data)) ) {
  //   $(id2clean).append('<option value="'+key+'">'+value+'</option>');
  // }
  sortByValue(data).forEach(function (item) {
    $(id2clean).append('<option value="'+item[1]+'">'+item[0]+'</option>');
  });
}


$( document ).ready(function() {
   var api_base = '/api';

//  var api_base = 'http://91.142.73.208:4000/api';

  var post_data = {};

  console.log( "ready!" );

  $.get( api_base + "/categories", function( data ) {
    createSelectOpts(data,'#floatingCategorySelect');
  });

  $.get( api_base + "/cities", function( data ) {
    createSelectOpts(data,'#floatingCitySelect');
  });

  // $('#floatingCategorySelect').change(function(){
  //   post_data.work_id = $('option:selected', this).val();
  //   post_data.name = $('option:selected', this).text();
  //   console.log("post_data: ", post_data);
  // });
  //
  // $('#floatingCitySelect').change(function(){
  //   post_data.city_id = $('option:selected', this).val();
  //   post_data.city_name = $('option:selected', this).text();
  //   console.log("post_data: ", post_data);
  // });

  // $(document).on('change', '#floatingCategorySelect, #floatingCitySelect, #people_n', function () {
  //    data = $('form').serializeJSON();
  //    console.log(data);
  // });

  $('#floatingCategorySelect, #floatingCitySelect, #people_n').change(function(){
    var form_data = $('form').serializeObject();
    if( form_data.work_id && form_data.city_id) {
      console.log("True");
      $.get( api_base + "/users/count/" + form_data.work_id + "/" + form_data.city_id, function( resp ) {
        // $('#users_count_alert').css('visibility','visible');
        $('#users_count_alert').removeClass( "alert-success alert-danger" );
        $('#users_count_alert').addClass("alert").empty();
        $('#users_count_alert').append('Доступно '+resp+' исполнителей');
        if (resp > 0) {
          $('#users_count_alert').addClass("alert-success");
        }
        else {
          $('#users_count_alert').addClass("alert-danger");
        }
        // console.log("Total:", resp);
      });
    }
  });

  $('#fillTestBtn').click(function(event){
    event.preventDefault();
    var test_data = {
        // "work_id": 31,
        // "work_name": "Caнтехника",
        // "city_id": 2,
        // "city_name": "Воронеж",
        "people_n": 3,
        // "dt_start": "2023-03-23 17:00",
        "dt_start": new Date().toJSON().slice(0,16),
        "address": "Лизюкова 4",
        "price_rub": 3000,
        "comment": "Нужно отремонтировать теплотрассу",
        // "additional_question_ids": [ 1, 2, 5 ]
    }
    for (const [key, value] of Object.entries(test_data) ) {
      $("[name='"+key+"']").val(value);
    }

    var test_data_2 = {
        "work_id": 7, // "Caнтехника",
        "city_id": 2,  // "Воронеж",
    };
    for (const [key, value] of Object.entries(test_data_2) ) {
      // el = $('select[name="work_id"]')
      $('select[name="'+key+'"] option[value=' + value + ']').attr('selected',true);
    }
  });


  $('#fillTestBtn2').click(function(event){
    event.preventDefault();
    var test_data = {
        "work_id": 132, // "Caнтехника"
        "city_id": 12, // "Москва"
        "people_n": 3,
        // "dt_start": "2023-03-23 17:00",
        "dt_start": new Date().toJSON().slice(0,16),
        "address": "Лизюкова 4",
        "price_rub": 3000,
        "comment": "Нужно отремонтировать теплотрассу",
        // "additional_question_ids": [ 1, 2, 5 ]
    }
    for (const [key, value] of Object.entries(test_data) ) {
      $("[name='"+key+"']").val(value);
    }

    var test_data_2 = {
      "work_id": 132, // "Caнтехника"
      "city_id": 12, // "Москва"
    };
    for (const [key, value] of Object.entries(test_data_2) ) {
      // el = $('select[name="work_id"]')
      $('select[name="'+key+'"] option[value=' + value + ']').attr('selected',true);
    }
  });




  $('#run_test_call').click(function(event){
    event.preventDefault();
    console.log("Test call runned");

    var form_data = $('form').serializeObject();
    form_data.work_name = $("[name='work_id']").find(":selected").text();
    form_data.city_name = $("[name='city_id']").find(":selected").text();
    form_data.phone = $("#test_phone").val();
    form_data.talker_name = $("#talker_name").val();

    // console.log(form_data);
    form_data = JSON.stringify(form_data);


    var url = api_base + "/test/call";
    // console.log(r);

    // $.post( url, form_data)
    //   .done(function( data ) {
    //   var href = "#";
    //   var txt = '<a href="'+href+'">Ссылка на результат</a>.<br>';
    //   txt += 'Откройте ссылку и обновите страничку через 5 минут и вы увидите оценку вероятности выхода на заказ';
    //   alert_selector = $('#test_call_id_alert');
    //   alert_selector.addClass("alert").empty();
    //   alert_selector.addClass("alert-success");
    //   alert_selector.append(txt);
    //   $("#run_test_call").attr("disabled", true);
    // });

    $.ajax({
      url: url,
      data:form_data,
      type:"POST",
      contentType:"application/json; charset=utf-8",
      dataType:"json",
      success: function(resp){
        var href = api_base + '/status/' + resp.job_id;
        var txt = '<a href="'+href+'">Ссылка на результат</a>.<br>';
        txt += 'Откройте ссылку и обновите страничку через 5 минут и вы увидите оценку вероятности выхода на заказ';
        alert_selector = $('#test_call_id_alert');
        alert_selector.addClass("alert").empty();
        alert_selector.addClass("alert-success");
        alert_selector.append(txt);
        $("#run_test_call").attr("disabled", true);
      }
    })


  });


  // target params: city_id, work_id, people_n etc.
  $('#newGroupCallBtn').click(function(event){
    // event.preventDefault();
    var form = $('form');
    if (form[0].checkValidity() === false) {
      event.preventDefault()
      event.stopPropagation()
    }
    var form_data = $('form').serializeObject();
    form_data.work_name = $("select[name='work_id']").find(":selected").text();
    form_data.city_name = $("select[name='city_id']").find(":selected").text();
    form_data = JSON.stringify(form_data);
    console.log(form_data);
    $.ajax({
      url: api_base + "/new",
      data: form_data,
      type: "POST",
      contentType: "application/json; charset=utf-8",
      dataType: "json",
      success: function(resp){
        console.log("RResponse", resp);
        window.location.href = resp['new_location'];
      },
      error: function (jqXHR, textStatus, errorThrown) {
        alert('Ошибка валидации. Проверьте что все поля формы заполнены')
      }
    })
  });
  // $('#people_n').change(function(){
  //   post_data.people_n = $(this).val();
  //   console.log("post_data: ", post_data);
  //
  //   $.get( api_base + "/users/count/" + post_data.work_id + "/" + post_data.city_id, function( data ) {
  //     console.log("Total:", post_data.city_id)
  //   });
  //   console.log($('form').serializeJSON())
  // });

  // problem with serializeJSON()
  // const exampleModal = document.getElementById('exampleModal')
  // exampleModal.addEventListener('show.bs.modal', event => {
  //   const modalBody = exampleModal.querySelector('#call_summary')
  //   modalBody.textContent = document.getElementById('main_form').serializeJSON()
  // })

  $("table").tablesorter({
    theme : "bootstrap",
  })

});
