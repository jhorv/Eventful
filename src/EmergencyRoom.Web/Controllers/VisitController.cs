﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Microsoft.FSharp.Core;

namespace EmergencyRoom.Web.Controllers
{
    public class VisitController : Controller
    {
        //
        // GET: /Visit/

        public ActionResult Index()
        {
            return View();
        }

        public ActionResult Register()
        {
            return View();
        }

        [AcceptVerbs(HttpVerbs.Post)]
        public ActionResult Register(RegisterPatientCommand command)
        {
            var errors = Visit.validateCommand(command);
            if (errors.Any())
            {
                foreach (var error in errors)
                {
                    var field = OptionModule.IsSome(error.Item1) ? error.Item1.Value : "";
                    var errorMessage = error.Item2;
                    ModelState.AddModelError(field, errorMessage);
                }
                return View();
            }
            else
            {
                return View("Success");
            }
        }
    }
}
